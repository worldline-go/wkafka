<script lang="ts">
  import { skip } from "@/helper/api";

  const skipFunc = () => {
    skip(topic, partition, offset);
  };

  export let topic: string;
  export let partition: number;
  export let offset: number;
</script>

<button
  class="bg-gray-600 hover:bg-red-600 text-white font-bold px-10 border-t border-b border-black"
  on:click|preventDefault|stopPropagation={skipFunc}
>
  Skip
</button>
