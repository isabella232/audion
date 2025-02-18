import {
  BehaviorSubject,
  combineLatest,
  concat,
  defer,
  EMPTY,
  Observable,
  of,
} from 'rxjs';
import {
  catchError,
  distinctUntilChanged,
  exhaustMap,
  filter,
  finalize,
  share,
  take,
} from 'rxjs/operators';

import {chrome} from '../chrome';
import {WebAudioDebuggerMethod} from '../chrome/DebuggerWebAudioDomain';

/**
 * Permission value in regards to calling `chrome.debugger.attach`.
 *
 * When the extension calls `chrome.debugger.attach` a notification will display
 * in devtools that the extension is debugging the tab. Attaching when the user
 * does not expect it and then see this notification is not desired. The user
 * needs to grant permission for the extension the privilege to attach, or
 * reject prior permission.
 *
 * Permission could be implied when the extension's panel is opened.
 *
 * Permission should be rejected when the debugging notification is canceled or
 * dismissed.
 *
 * Permission could be granted more explicitly by a panel component when the
 * panel is visible but the extension does not have permission.
 *
 * WebAudioEventObserver will be instructed with rules like the above by other
 * functions outside of this file.
 */
enum AttachPermission {
  /**
   * Initial value.
   *
   * When WebAudioEventObserver is created, it does not know if permission has
   * been granted or not and should treat this as **not having** permission.
   */
  UNKNOWN,

  /**
   * Permission has been granted by a user action. WebAudioEventObserver may
   * attach to `chrome.debugger`.
   */
  TEMPORARY,

  /**
   * Permission has been rejected. WebAudioEventObserver must not attach to
   * `chrome.debugger`.
   */
  REJECTED,
}

/**
 * Value used to indicate if the `chrome.debugger` attachment and
 * receiving `chrome.debugger.onEvent` events are "active".
 */
enum BinaryTransition {
  DEACTIVATING = 'deactivating',
  IS_INACTIVE = 'isInactive',
  ACTIVATING = 'activating',
  IS_ACTIVE = 'isActive',
}

export interface DebuggerAttachEventState {
  permission: AttachPermission;
  attachInterest: number;
  attachState: BinaryTransition;
  webAudioEventInterest: number;
  webAudioEventState: BinaryTransition;
}

/** Chrome Devtools Protocol version to attach to. */
const debuggerVersion = '1.3';

/** Chrome tab to attach the debugger to. */
const {tabId} = chrome.devtools.inspectedWindow;

/**
 * Control attachment to chrome.debugger depending on if the user has given
 * permission and how many parts of the extension need attachment.
 *
 * @memberof Audion
 * @alias DebuggerAttachEventController
 */
export class DebuggerAttachEventController {
  /** Does user permit extension to use `chrome.debugger`. */
  permission$: PermissionSubject;
  /** How many subscriptions want to attach to `chrome.debugger`. */
  attachInterest$: CounterSubject;
  attachState$: Observable<BinaryTransition>;
  /**
   * How many subscriptions want to receive web audio events through
   * `chrome.debugger.onEvent`.
   */
  webAudioEventInterest$: CounterSubject;
  webAudioEventState$: Observable<BinaryTransition>;

  combinedState$: Observable<DebuggerAttachEventState>;

  constructor() {
    // Create an interface of subjects to track changes in state with the
    // `chrome.debugger` api.
    const debuggerSubject = {
      // Does the extension have permission from the user to use `chrome.debugger` api.
      permission: new PermissionSubject(),
      // How many entities want to attach to the debugger to call `sendCommand`
      // or listen to `onEvent`.
      attachInterest: new CounterSubject(0),
      // attachState must be IS_ACTIVE for `chrome.debugger.sendCommand` to be used.
      attachState: new BinaryTransitionSubject({
        initialState: BinaryTransition.IS_INACTIVE,
        activateAction: () => attach({tabId}, debuggerVersion),
        deactivateAction: () => detach({tabId}),
      }),
      // How many entities want to listen to web audio events through `onEvent`.
      webAudioEventInterest: new CounterSubject(0),
      // webAudioEventState must be IS_ACTIVE for `onEvent` to receive events.
      webAudioEventState: new BinaryTransitionSubject({
        initialState: BinaryTransition.IS_INACTIVE,
        activateAction: () =>
          sendCommand({tabId}, WebAudioDebuggerMethod.enable),
        deactivateAction: () =>
          sendCommand({tabId}, WebAudioDebuggerMethod.disable),
      }),
    };
    this.permission$ = debuggerSubject.permission;
    this.attachInterest$ = debuggerSubject.attachInterest;
    this.attachState$ = debuggerSubject.attachState;
    this.webAudioEventInterest$ = debuggerSubject.webAudioEventInterest;
    this.webAudioEventState$ = debuggerSubject.webAudioEventState;

    // Observable of changes to state derived from debuggerSubject.
    const debuggerState$ = (this.combinedState$ =
      // Push objects mapping of keys in debuggerSubject to values pushed from
      // that debuggerSubject member.
      combineLatest(debuggerSubject).pipe(
        // Filter out combined state that is not different from the last value.
        distinctUntilChanged(
          (previous, current) =>
            previous.permission === current.permission &&
            previous.attachInterest === current.attachInterest &&
            previous.attachState === current.attachState &&
            previous.webAudioEventInterest === current.webAudioEventInterest &&
            previous.webAudioEventState === current.webAudioEventState,
        ),
        // Make one subscription debuggerSubject once for many subscribers.
        share(),
      ));

    // The following subscriptions govern debuggerSubject.

    // Govern attachment to `chrome.debugger`.
    debuggerState$.subscribe({
      next: (state) => {
        // When debugger state has permission to attach to `chrome.debugger` and
        // something wants to use `chrome.debugger`, activate the attachment.
        // Otherwise deactivate the attachment.
        if (
          state.permission === AttachPermission.TEMPORARY &&
          state.attachInterest > 0
        ) {
          debuggerSubject.attachState.activate();
        } else {
          debuggerSubject.attachState.deactivate();
        }
      },
    });

    // Govern permission rejection and externally induced detachment.
    onDebuggerDetach$.subscribe({
      next([, reason]) {
        if (reason === 'canceled_by_user') {
          // Reject permission to use `chrome.debugger` in this extension. We
          // understand this event to be an explicit rejection from the
          // extension's user.
          debuggerSubject.permission.reject();
        }

        // Immediately go to the inactive state. Detachment was initiated
        // outside the extension and does not need to be requested.
        debuggerSubject.attachState.next(BinaryTransition.IS_INACTIVE);
      },
    });

    // Govern receiving web audio events through `chrome.debugger.onEvent`.
    debuggerState$.subscribe({
      next(state) {
        if (
          state.attachState === BinaryTransition.IS_ACTIVE &&
          state.webAudioEventInterest > 0
        ) {
          // Start receiving events. The attachemnt is active and some entities
          // are listeneing for events.
          debuggerSubject.webAudioEventState.activate();
        } else {
          if (state.attachState === BinaryTransition.IS_ACTIVE) {
            // Stop receiving events. The attachment is still active but no
            // entities are listening for events.
            debuggerSubject.webAudioEventState.deactivate();
          } else {
            // "Skip" deactivation of receiving events and immediately go to
            // the inactive state. The process of detachment either requested by
            // the extension or initiated otherwise has implicitly stopped
            // reception of events.
            debuggerSubject.webAudioEventState.next(
              BinaryTransition.IS_INACTIVE,
            );
          }
        }
      },
    });
  }

  /**
   * Attach to the debugger if not already, and call chrome.debugger.sendCommand.
   * @param method Chrome devtools protocol method like 'HeapProfiler.collectGarbage'.
   * @returns observable that completes once done without pushing any values
   */
  sendCommand(method: string): Observable<never> {
    this.attachInterest$.increment();
    return this.attachState$.pipe(
      filter((state) => state === BinaryTransition.IS_ACTIVE),
      take(1),
      exhaustMap(() => sendCommand({tabId}, method)),
      finalize(() => this.attachInterest$.decrement()),
    );
  }
}

/**
 * Create a function that returns an observable that completes when the api
 * calls back.
 * @param method `chrome` api method whose last argument is a callback
 * @param thisArg `this` inside of the method
 * @returns observable that completes when the method is done
 */
function bindChromeCallback<P extends any[]>(
  method: (...args: [...params: P, callback: () => void]) => void,
  thisArg = null,
) {
  return (...args: P) =>
    new Observable<never>((subscriber) => {
      method.call(thisArg, ...args, () => {
        if (chrome.runtime.lastError) {
          subscriber.error(chrome.runtime.lastError);
        } else {
          subscriber.complete();
        }
      });
    });
}

/**
 * Return an observable that pushes events from a `chrome` api event.
 * @param event `chrome` api event
 * @returns observable of `chrome` api events
 */
function fromChromeEvent<A extends any[]>(
  event: Chrome.Event<(...args: A) => any>,
) {
  return new Observable<A>((subscriber) => {
    const listener = (...args: A) => {
      subscriber.next(args);
    };
    event.addListener(listener);
    return () => {
      event.removeListener(listener);
    };
  });
}

/**
 * Call `chrome.debugger.attach`.
 *
 * @see
 * https://developer.chrome.com/docs/extensions/reference/debugger/#method-attach
 */
const attach = bindChromeCallback(chrome.debugger.attach, chrome.debugger);

/**
 * Call `chrome.debugger.detach`.
 *
 * @see
 * https://developer.chrome.com/docs/extensions/reference/debugger/#method-detach
 */
const detach = bindChromeCallback(chrome.debugger.detach, chrome.debugger);

/**
 * Call `chrome.debugger.sendCommand`.
 *
 * @see
 * https://developer.chrome.com/docs/extensions/reference/debugger/#method-sendCommand
 */
const sendCommand = bindChromeCallback(
  chrome.debugger.sendCommand as (
    target: Chrome.DebuggerDebuggee,
    method: string,
    params?,
    callback?,
  ) => void,
  chrome.debugger,
);

/**
 * Observable of `chrome.debugger.onDetach` events.
 */
const onDebuggerDetach$ = fromChromeEvent<
  [target: Chrome.DebuggerDebuggee, reason: string]
>(chrome.debugger.onDetach);

/**
 * Store if user allows the extension to use `chrome.debugger` api.
 */
export class PermissionSubject extends BehaviorSubject<AttachPermission> {
  constructor() {
    super(AttachPermission.UNKNOWN);
  }

  /**
   * Permit use of `chrome.debugger`.
   */
  grantTemporary() {
    if (this.value === AttachPermission.UNKNOWN) {
      this.next(AttachPermission.TEMPORARY);
    }
  }

  /**
   * Reject use of `chrome.debugger`.
   */
  reject() {
    if (this.value !== AttachPermission.REJECTED) {
      this.next(AttachPermission.REJECTED);
    }
  }
}

/**
 * Description of a transition in BinaryTransitionSubject.
 */
interface BinaryTransitionDescription {
  /** The state the Subject must start in to perform this transition. */
  beginningState: BinaryTransition;
  /** The state the Subject is in while performing this transition. */
  intermediateState: BinaryTransition;
  /** The state the Subject is in after action is successfully. */
  successState: BinaryTransition;
  /** The state the Subject is in after action is unsuccessful. */
  errorState: BinaryTransition;
  /**
   * Delegate that does some work to modify other application state to the
   * desired state.
   */
  action: () => Observable<void>;
}

/**
 * Control a transition between inactive and active state. To perform a
 * transition the subject enters a intermediate state and calls a delegate to do
 * some action. After the action completes successfully the subject enters the
 * desired state.
 */
class BinaryTransitionSubject extends BehaviorSubject<BinaryTransition> {
  private readonly activateTransition: BinaryTransitionDescription;
  private readonly deactivateTransition: BinaryTransitionDescription;

  constructor({
    initialState,
    activateAction,
    deactivateAction,
  }: {
    initialState: BinaryTransition;
    activateAction: () => Observable<void>;
    deactivateAction: () => Observable<void>;
  }) {
    super(initialState);
    this.activateTransition = {
      beginningState: BinaryTransition.IS_INACTIVE,
      intermediateState: BinaryTransition.ACTIVATING,
      successState: BinaryTransition.IS_ACTIVE,
      errorState: BinaryTransition.IS_INACTIVE,
      action: activateAction,
    };
    this.deactivateTransition = {
      beginningState: BinaryTransition.IS_ACTIVE,
      intermediateState: BinaryTransition.DEACTIVATING,
      successState: BinaryTransition.IS_INACTIVE,
      errorState: BinaryTransition.IS_INACTIVE,
      action: deactivateAction,
    };
  }

  /**
   * Transition to a desired state.
   *
   * Change the subject value if it is set to beginningState to intermediateState and once action completes successfuly, set to successState.
   * @param description
   */
  transition(description: BinaryTransitionDescription) {
    if (this.value === description.beginningState) {
      concat(
        of(description.intermediateState),
        description.action(),
        defer(() =>
          this.value === description.intermediateState
            ? of(description.successState)
            : EMPTY,
        ),
      )
        .pipe(
          catchError((err) => {
            console.error(err);
            if (
              err.message.startsWith('Another debugger is already attached')
            ) {
              return this.value === description.intermediateState
                ? of(description.successState)
                : EMPTY;
            }
            return of(
              this.value === description.intermediateState
                ? description.errorState
                : description.beginningState,
            );
          }),
        )
        .subscribe({next: this.next.bind(this)});
    }
  }

  /**
   * If subject is inactive, transition to active.
   */
  activate() {
    this.transition(this.activateTransition);
  }

  /**
   * If subject is active, transition to inactive.
   */
  deactivate() {
    this.transition(this.deactivateTransition);
  }
}

/**
 * Observable counting some discrete value.
 */
export class CounterSubject extends BehaviorSubject<number> {
  /**
   * Increase value by 1.
   */
  increment() {
    this.next(this.value + 1);
  }

  /**
   * Decrease value by 1.
   */
  decrement() {
    this.next(this.value - 1);
  }
}
