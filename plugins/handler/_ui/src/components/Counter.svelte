<script lang="ts">
  import { countDownTimer } from "@/helper/counter";
  import { onDestroy } from "svelte";

  export let dateTo = "";

  let counter = "";
  let timer: number;

  onDestroy(() => {
    clearInterval(timer);
  });

  const startTimer = (dt: string) => {
    let end = Date.parse(dt);

    timer = countDownTimer(end, (v: string) => {
      counter = v;
    });
  };

  $: startTimer(dateTo);
</script>

{counter}
