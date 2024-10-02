import { writable } from "svelte/store";

let navbar = {
  title: "",
  sideBarOpen: false
};

export const storeNavbar = writable(navbar);
