import {registerGlobalInitFunc} from '../modules/observer.ts';

export function initMailboxMoveForm() {
  registerGlobalInitFunc('initMailboxMoveForm', (form: HTMLFormElement) => {
    form.querySelector('select[name="folder"]')!.addEventListener('change', () => form.submit());
  });
}
