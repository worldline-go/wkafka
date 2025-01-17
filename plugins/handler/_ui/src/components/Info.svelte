<script lang="ts">
  import { storeInfo } from "@/store/store";
  import TreeView from "svelte-tree-view";
  import Action from "./Action.svelte";
  import { getField, getFieldWithDecode } from "@/helper/codec";
  import View from "./View.svelte";
  import type { Info } from "@/store/model";
  import { emptyDLQBanner } from "@/helper/banner";
  import Counter from "./Counter.svelte";
  import Instance from "./Instance.svelte";
  import { skipClear } from "@/helper/api";

  const DLQExists = (storeInfo: Record<string, Info>) => {
    for (const [_, value] of Object.entries(storeInfo)) {
      if (value.dlq_record) {
        return true;
      }
    }

    return false;
  };
</script>

<div class="font-mono flex flex-col md:flex-row gap-1 h-full w-full">
  <div class="flex-1">
    <span class="block bg-yellow-100 border-b border-gray-600 mb-1">
      DLQ Stuck Messages
    </span>

    {#if !DLQExists($storeInfo)}
      <span class="text-gray-500">
        <pre>{emptyDLQBanner}</pre>
      </span>
    {/if}

    {#each Object.entries($storeInfo) as [key, value]}
      {#if value.dlq_record}
        <fieldset class="border border-solid border-gray-300 p-2">
          <legend class="text-black bg-gray-300 px-1">
            DLQ Message on {key}
          </legend>
          <div class="text-sm">
            <details class="bg-gray-200">
              <summary>Message Tree View</summary>
              <TreeView data={value.dlq_record} />
            </details>
            <div>
              <span class="block">
                <b>{getFieldWithDecode("process", value.dlq_record.headers)}</b>
                at
                <i>{value.dlq_record.timestamp}</i>
                <span class="text-gray-600 block">
                  └───
                  <span class="bg-gray-600 text-white px-1">
                    {value.dlq_record.topic}
                  </span>
                  partition
                  <span class="bg-gray-600 text-white px-1">
                    {value.dlq_record.partition}
                  </span>
                  offset
                  <span class="bg-gray-600 text-white px-1">
                    {value.dlq_record.offset}
                  </span>
                </span>
              </span>
              <View value64={value.dlq_record.value} title="Record" />
              <View
                value64={getField("error", value.dlq_record.headers)}
                title="Record Error"
              />
              <hr class="my-2" />
              <View value={value.error} title="Process Error" />
            </div>
          </div>
          <div class="flex flex-row justify-between items-baseline">
            <div class="mt-2">
              <Action
                topic={value.dlq_record.topic}
                partition={value.dlq_record.partition}
                offset={value.dlq_record.offset}
              />
            </div>
            <span class="bg-gray-200 px-2" title="Retry At">
              <Counter dateTo={value.retry_at} />
            </span>
          </div>
        </fieldset>
      {/if}
    {/each}
  </div>

  <hr class="md:hidden my-2" />

  <div class="md:border-l px-1 min-w-96">
    <div class="mb-1">
      <button
        class="md:w-full bg-gray-600 hover:bg-yellow-500 text-white font-bold px-10 border-t border-b border-black"
        on:click|preventDefault|stopPropagation={skipClear}
      >
        Clear Skip Configs
      </button>
    </div>
    <div>
      <span class="block bg-yellow-100 border-b border-gray-600 mb-1">
        Instances
      </span>

      <span>
        Total Instances:
        <span class="px-2 bg-gray-600 text-white">
          {Object.keys($storeInfo).length}
        </span>
      </span>

      <hr class="mb-1" />

      <div>
        {#each Object.entries($storeInfo) as [key, value]}
          <Instance {key} {value} />
        {/each}
      </div>
    </div>
  </div>
</div>
