<script lang="ts">
  import Link from "./Link.svelte";

  let className = "";
  export { className as class };
</script>

<div
  class={`${className} bg-black text-white flex items-center border-b border-gray-500 relative`}
>
  <Link path="/" name="wkafka" />
  <Link path="/swagger" name="swagger" />

  <a
    href="https://github.com/worldline-go/wkafka"
    title="wkafka repo"
    target="_blank"
    rel="noopener noreferrer"
    class="h-full px-2 absolute right-0 text-gray-300 hover:bg-yellow-300 hover:text-black"
  >
    repo
  </a>
</div>
