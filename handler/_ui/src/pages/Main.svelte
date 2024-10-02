<script lang="ts">
  import { storeNavbar } from "@/store/store";
  import update from "immutability-helper";

  storeNavbar.update((v) => update(v, { title: { $set: "Main" } }));
</script>

<div class="flex w-full h-full justify-center align-middle">
  <span class="text-2xl self-center">Welcome!</span>
</div>
