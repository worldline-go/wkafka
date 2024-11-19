<script lang="ts">
  import type { OffsetConfig } from "@/store/model";

  export let skip: Map<string, Map<number, OffsetConfig>> | undefined;
</script>

<div>
  {#if skip}
    {#each Array.from(skip.entries()) as [outerKey, innerMap]}
      <div class="outer-item">
        <h3>Outer Key: {outerKey}</h3>
        {#if innerMap instanceof Map}
          {#each Array.from(innerMap.entries()) as [innerKey, offsetConfig]}
            <div class="inner-item">
              <p>Inner Key: {innerKey}</p>
              <p>Offset Config: {JSON.stringify(offsetConfig)}</p>
            </div>
          {/each}
        {/if}
      </div>
    {/each}
  {/if}
</div>
