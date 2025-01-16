<script lang="ts">
  import { base64ToStr, parseJSON } from "@/helper/codec";
  import { onMount } from "svelte";
  import TreeView from "svelte-tree-view";

  export let title = "";
  export let value64 = "";
  export let value = "";

  let toggle = false;
  let jsonValue: unknown;
  let className = "";

  export { className as class };

  onMount(() => {
    if (value64 != "") {
      value = base64ToStr(value64);
    }

    jsonValue = parseJSON(value);

    if (!!jsonValue) {
      toggle = true;
    }
  });
</script>

<fieldset class="border border-solid border-gray-300 px-2 {className}">
  <legend class="px-1">
    <span class="text-black bg-gray-300 px-1">{title.padEnd(12, "-")}</span>
    <label class="relative inline-flex items-center cursor-pointer">
      <input type="checkbox" class="sr-only peer" bind:checked={toggle} />
      <div
        class="w-10 h-2 bg-gray-200 peer peer-focus:ring-4 peer-focus:ring-orange-300 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:h-4 after:w-4 after:transition-all peer-checked:bg-orange-600"
      ></div>

      <span class="ml-3 text-sm font-medium text-gray-900">
        {toggle ? "JSON" : "Raw"}</span
      >
    </label>
  </legend>
  <span class={toggle ? "hidden" : ""}>{value}</span>
  <span class={toggle ? "" : "hidden"}>
    <TreeView data={jsonValue} />
  </span>
</fieldset>
