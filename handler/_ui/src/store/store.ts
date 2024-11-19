import { writable } from "svelte/store";
import type { Info } from "./model";

let navbar = {
  title: "",
};

export const storeNavbar = writable(navbar);
export const storeInfo = writable({} as Info);
