<script lang="ts">
  import active from "svelte-spa-router/active";

  export let path = "/";
  export let name = "";
</script>

<a
  href={`#${path}`}
  class="h-full hover:bg-yellow-500 hover:text-white"
  use:active={{
    path: path,
    className: "bg-green-500 text-gray-100",
    inactiveClassName: "bg-black text-gray-100",
  }}
>
  <span class="px-2">{name}</span>
</a>
