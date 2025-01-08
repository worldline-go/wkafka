<script lang="ts">
  import type { ComponentType } from "svelte";
  import { onMount } from "svelte";
  import Router from "svelte-spa-router";

  import MainPage from "@/pages/Main.svelte";
  import Swagger from "@/pages/Swagger.svelte";
  import NotFound from "@/pages/NotFound.svelte";

  import Toast from "@/components/Toast.svelte";
  import Navbar from "@/components/Navbar.svelte";

  let mounted = false;

  const routes = new Map<string | RegExp, ComponentType>();
  routes.set("/swagger", Swagger);
  routes.set("/", MainPage);
  routes.set("/*", NotFound);

  onMount(async () => {
    mounted = true;
  });
</script>

<Toast />

<div class="grid h-full w-full relative overflow-y-auto bg-slate-100">
  {#if !mounted}
    <div class="absolute inset-0 flex items-center justify-center">
      <div
        class="animate-spin rounded-full h-32 w-32 border-t-2 border-b-2 border-gray-900"
      ></div>
    </div>
  {:else}
    <div class="h-full w-full grid grid-rows-[1.75rem]">
      <Navbar />
      <Router {routes} />
    </div>
  {/if}
</div>
