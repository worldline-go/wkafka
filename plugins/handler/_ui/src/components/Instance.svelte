<script lang="ts">
  import TreeView from "svelte-tree-view";

  export let key = "";
  export let value = {};

  let toogle = false;
</script>

<div>
  <button
    class={`w-full text-left text-black px-1 border-t border-gray-600 hover:bg-yellow-200 ${
      toogle ? "bg-gray-200 border-l-4" : "bg-gray-300"
    }`}
    on:click={() => (toogle = !toogle)}>{key}</button
  >
  <span
    class={`block border-l-4 border-gray-600 relative ${toogle ? "" : "hidden"}`}
  >
    <TreeView data={value} />
  </span>
</div>
