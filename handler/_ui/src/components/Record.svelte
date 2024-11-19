<script lang="ts">
  import type { DlqRecord } from "@/store/model";

  export let dlqRecord = null as null | DlqRecord;
</script>

<div>
  {#if dlqRecord}
    <span>Topic: {dlqRecord.topic}</span>
  {/if}
</div>
