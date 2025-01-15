<script lang="ts">
  import { storeNavbar } from "@/store/store";
  import update from "immutability-helper";

  storeNavbar.update((v) => update(v, { title: { $set: "404 - Not Found" } }));
</script>

<h1>Not Found</h1>
