import {defer, Observable, of, OperatorFunction, pipe} from 'rxjs';
import {finalize, map, scan, switchMap} from 'rxjs/operators';

/**
 * Helper function to create a function to modify an element when data changes.
 * @param applyChange change an element
 * @param removeChange remove the change
 * @returns observable factory that takes an observable of elements and
 * observable of some data
 */
function changeElement<E extends HTMLElement, T>(
  applyChange: (element: E, data: T) => void,
  removeChange: (element: E, lastData: T) => void,
) {
  return function (element$: Observable<E>, data$: Observable<T>) {
    return element$.pipe(
      switchMap((view) => {
        let lastValue: T;
        return data$.pipe(
          map((value) => {
            if (view) {
              lastValue = value;
              applyChange(view, value);
            }
            return view;
          }),
          finalize(() => {
            if (view) {
              removeChange(view, lastValue);
            }
          }),
        );
      }),
    );
  };
}

/**
 * Create a factory that modifies the most latest element from an observable of
 * elements to value from an observable of other values.
 * @param property html element property
 * @returns factory that modifies a latest element with the latest data
 */
export function setElementProperty<
  E extends HTMLElement,
  K extends keyof E,
  T extends E[K],
>(property: K) {
  return changeElement<E, T>(
    (view, data) => {
      view[property] = data;
    },
    (view) => {
      view[property] = null;
    },
  );
}

/**
 * Set that values can be added to and removed from.
 */
interface PropertySet<T> {
  add(value: T): any;
  remove(value: T): any;
}

/**
 * Description of a change to a PropertySet.
 */
interface PropertySetChange {
  /** Items to remove from the PropertySet. */
  deleteItems: string[];
  /** Items to add to the PropertySet. */
  addItems: string[];
  /** All items to remove if the element changes or finalizes. */
  allItems: string[];
}

/**
 * Create a factory that adds and removes the items contained in a observable of
 * array values to the latest element.
 * @param property html element property
 * @returns factory that adds and removes items on an elements property
 */
export function toggleElementPropertySet<
  E extends HTMLElement,
  K extends {
    [key in keyof E]: E[key] extends PropertySet<string> ? key : never;
  }[any],
  T extends string[],
>(property: K) {
  const changeElementProperty = changeElement(
    (view, diff: PropertySetChange) => {
      console.log(view, diff);
      for (const value of diff.deleteItems) {
        (view[property] as PropertySet<string>).remove(value);
      }
      for (const value of diff.addItems) {
        (view[property] as PropertySet<string>).add(value);
      }
    },
    (view, diff) => {
      for (const value of diff.allItems) {
        (view[property] as PropertySet<string>).remove(value);
      }
    },
  );
  return function (element$: Observable<E>, data$: Observable<T>) {
    return changeElementProperty(element$, data$.pipe(mapSetChange<T>()));
  };
}

/**
 * Observable operator function that maps a string array to the difference in
 * string values from the last value.
 */
function mapSetChange<T extends string[]>(): OperatorFunction<
  T,
  PropertySetChange
> {
  return pipe(
    scan<T, [T, PropertySetChange]>(
      ([previous], current) => {
        const allItems = current;
        const deleteItems = previous.filter(
          (value) => !current.includes(value),
        );
        const addItems = allItems.filter((value) => !previous.includes(value));

        return [current, {deleteItems, addItems, allItems}] as [
          T,
          PropertySetChange,
        ];
      },
      [[], {deleteItems: [], addItems: []}] as [T, PropertySetChange],
    ),
    map(([, change]) => change),
  );
}

/**
 * Change to a html element property's map structure.
 */
interface PropertyMapChange {
  /** Keys to remove from the property's map. */
  deleteKeys: string[];
  /** Keys to change to a given value. */
  setKeys: [string, any][];
  /** All keys. Used to remove all keys when the element changes or finalizes. */
  allKeys: string[];
}

/**
 * Set latest element's properties to the latest map of values.
 */
export function assignElementProperty<
  E extends HTMLElement,
  K extends keyof E,
  T extends {[key in keyof E[K]]?: E[K][key]},
>(property: K) {
  const changeElementProperties = changeElement(
    (view: E, diff: PropertyMapChange) => {
      if (view) {
        for (const key of diff.deleteKeys) {
          view[property][key] = undefined;
        }
        for (const [key, value] of diff.setKeys) {
          view[property][key] = value;
        }
      }
    },
    (view, diff: PropertyMapChange) => {
      if (view) {
        for (const key of diff.allKeys) {
          view[property][key] = undefined;
        }
      }
    },
  );
  return function (element$: Observable<E>, data$: Observable<T>) {
    return changeElementProperties(element$, data$.pipe(mapMapChange()));
  };
}

function mapMapChange<T extends {[key: string]: any}>() {
  return pipe(
    scan<T, [T, PropertyMapChange]>(
      ([previous], current) => {
        const allKeys = Object.keys(current);
        const deleteKeys = Object.keys(previous).filter(
          (key) => !(key in current),
        );
        const setKeys = allKeys
          .filter((key) => current[key] !== previous[key])
          .map((key) => [key, current[key]]);

        return [current, {deleteKeys, setKeys, allKeys}] as [
          T,
          PropertyMapChange,
        ];
      },
      [{}, {deleteKeys: [], setKeys: []}] as [T, PropertyMapChange],
    ),
    map(([, change]) => change),
  );
}

/**
 * Set latest element's childNodes property to the latest array of nodes.
 */
export const setElementChildNodes = changeElement<HTMLElement, Node[]>(
  (element, childNodes) => {
    while (element.childNodes.length) {
      element.removeChild(element.childNodes[element.childNodes.length - 1]);
    }
    for (let i = 0; i < childNodes.length; i++) {
      element.appendChild(childNodes[i]);
    }
  },
  (element) => {
    while (element.childNodes.length) {
      element.removeChild(element.childNodes[element.childNodes.length - 1]);
    }
  },
);

/**
 * Set latest element's innerText property to latest data string value.
 */
export const setElementText = setElementProperty('innerText');

/**
 * Set latest element's innerHTML property to latest data string value.
 */
export const setElementHTML = setElementProperty('innerHTML');

/**
 * Set latest element's className property to latest data string value.
 */
export const setElementClassName = setElementProperty('className');

/**
 * Add and remove latest data string array to latest element's classList set
 * property.
 */
export const toggleElementClassList = toggleElementPropertySet('classList');

/**
 * Set and delete changes keys of latest data object to latest element's style
 * object map property.
 */
export const assignElementStyle = assignElementProperty('style');

/**
 * @param query css query selector to find an element for
 * @param dom document to query
 * @returns observable of a html element matching the query
 */
export function querySelector(
  query: string,
  dom: {querySelector(...args: any): any} = document,
): Observable<HTMLElement> {
  return defer(() => of(dom.querySelector(query)));
}

/**
 * @param html template content to create an element from
 * @param dom document to create elements under
 * @returns observable of a html element made from the html parameter
 */
export function createFragment<T extends HTMLElement>(
  html: string,
  dom: {createElement(tagName: 'template'): HTMLTemplateElement} = document,
): Observable<T> {
  return defer(() => {
    const template = dom.createElement('template');
    template.innerHTML = html;
    return of(template.content.firstElementChild as T);
  });
}
