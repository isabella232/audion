import {
  combineLatest,
  from,
  fromEvent,
  merge,
  NEVER,
  Observable,
  of,
  pipe,
} from 'rxjs';
import {
  finalize,
  ignoreElements,
  map,
  shareReplay,
  startWith,
  switchMap,
  tap,
  withLatestFrom,
} from 'rxjs/operators';

import {Audion} from '../../devtools/Types';

import {
  createFragment,
  setElementChildNodes,
  toggleElementClassList,
} from './domUtils';

import styles from './debugBar.css';

export function renderDebugBar(
  barElement$: Observable<HTMLElement>,
  currentGraph$: Observable<Audion.GraphContext>,
): Observable<HTMLElement | Audion.DebugAction> {
  const copyButtonElement$ = createFragment(
    '<button>Copy Context</button>',
  ).pipe(shareReplay());
  const downloadElement$ = createFragment<HTMLAnchorElement>(
    '<a href="" download disabled>Download</a>',
  ).pipe(shareReplay());
  const updateButtonElement$ = createFragment(
    '<button>Update Context</button>',
  ).pipe(shareReplay());
  const contextInputElement$ = createFragment<HTMLInputElement>(
    '<input type="file">',
  ).pipe(shareReplay());

  const childElements$ = merge(
    NEVER,
    combineLatest([
      copyButtonElement$,
      downloadElement$,
      updateButtonElement$,
      contextInputElement$,
    ]),
  );

  return merge(
    toggleElementClassList(
      barElement$,
      NEVER.pipe(startWith([styles.debugBar])),
    ),
    setElementChildNodes(barElement$, childElements$),
    copyButtonElement$.pipe(copyButtonAction(currentGraph$, downloadElement$)),
    updateButtonElement$.pipe(updateButtonAction(contextInputElement$)),
  );
}

function updateButtonAction(
  contextInputElement$: Observable<HTMLInputElement>,
) {
  return pipe(
    switchMap((element: HTMLElement) => fromEvent(element, 'click')),
    withLatestFrom(contextInputElement$),
    switchMap(([, inputElement]) => from(inputElement.files[0].text())),
    map((data) => JSON.parse(data) as Audion.GraphContext),
    map(
      (graphContext) =>
        ({
          type: Audion.DebugActionType.UPDATE,
          graphContext,
        } as Audion.DebugUpdateContextAction),
    ),
  );
}

function copyButtonAction(
  currentGraph$: Observable<Audion.GraphContext>,
  downloadElement$: Observable<HTMLAnchorElement>,
) {
  return pipe(
    switchMap((element: HTMLButtonElement) => fromEvent(element, 'click')),
    withLatestFrom(currentGraph$),
    map(([, graphContext]) => graphContext),
    mapToFile(
      (graphContext, index) =>
        `debug-audion-${graphContext.id.slice(-6)}-${index}.json`,
    ),
    withLatestFrom(downloadElement$),
    tap<[{file: File; url: string}, HTMLAnchorElement]>(
      ([graphContextFile, downloadElement]) => {
        downloadElement.href = graphContextFile.url;
        downloadElement.innerText = `Download - ${graphContextFile.file.name}`;
        downloadElement.download = graphContextFile.file.name;
        downloadElement.removeAttribute('disabled');
      },
    ),
    ignoreElements(),
  );
}

function mapToFile<V>(getFileName: (value: V, index: number) => string) {
  return pipe(
    map(
      (graphContext: V, index) =>
        new File(
          [JSON.stringify(graphContext)],
          getFileName(graphContext, index),
          {type: 'application/json'},
        ),
    ),
    map((graphContextFile) => ({
      file: graphContextFile,
      url: URL.createObjectURL(graphContextFile),
    })),
    switchMap((file) =>
      NEVER.pipe(
        startWith(file),
        // When a new file is pushed switchMap will unsubscribe from this last
        // observable returned observable calling the finalize callback,
        // revoking the url.
        finalize(() => URL.revokeObjectURL(file.url)),
      ),
    ),
  );
}
