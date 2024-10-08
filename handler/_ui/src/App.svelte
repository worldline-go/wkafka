<script lang="ts">
  import { onMount } from "svelte";

  import Toast from "./components/Toast.svelte";
  import MainPage from "@/pages/Main.svelte";

  let mounted = false;

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
    <MainPage />
  {/if}
</div>
