import { writable } from "svelte/store";
import type { Info } from "./model";
import update from "immutability-helper";

let navbar = {
  title: "",
};

export const storeNavbar = writable(navbar);
export const storeInfo = writable({} as Record<string, Info>);

export const storeInfoAdd = (info: Info) => {
  info.updated_at = Date.now();

  storeInfo.update((v) => update(v, { [info.id]: { $set: info } }));
}

export const storeInfoDelete = (id: string) => {
  storeInfo.update((v) => update(v, { $unset: [id] }));
}

export const storeInfoClear = () => {
  storeInfo.update((_) => ({}));
}
