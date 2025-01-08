<script lang="ts">
  import { onMount, onDestroy } from "svelte";
  import update from "immutability-helper";
  import { eventInfoListener } from "@/helper/api";
  import { storeNavbar, storeInfo } from "@/store/store";
  import Skip from "@/components/Skip.svelte";

  storeNavbar.update((v) => update(v, { title: { $set: "wkafka" } }));
  let event: EventSource;

  onMount(() => {
    event = eventInfoListener();
  });

  onDestroy(() => {
    event.close();
  });
</script>

<div>
  <span class="block">Listening topics {$storeInfo.topics}</span>
  <span class="block">Listening DLQ topics {$storeInfo.dlq_topics}</span>
  <!-- <Skip skip={$storeInfo.skip} /> -->
</div>
