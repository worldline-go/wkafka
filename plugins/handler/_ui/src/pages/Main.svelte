<script lang="ts">
  import { onMount, onDestroy } from "svelte";
  import update from "immutability-helper";
  import { eventInfoListener } from "@/helper/api";
  import { storeNavbar } from "@/store/store";
  import Info from "@/components/Info.svelte";

  storeNavbar.update((v) => update(v, { title: { $set: "wkafka" } }));
  let event: EventSource;

  onMount(() => {
    event = eventInfoListener();
  });

  onDestroy(() => {
    event.close();
  });
</script>

<div class="m-2">
  <Info />
</div>
