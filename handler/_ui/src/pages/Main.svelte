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

<!-- <div class="flex w-full h-full justify-center align-middle">
  <span class="text-2xl self-center">Welcome!</span>
</div> -->

<div>
  <span class="block">Listening topics {$storeInfo.topics}</span>
  <span class="block">Listening DLQ topics {$storeInfo.dlq_topics}</span>
  <Skip skip={$storeInfo.skip} />
</div>
