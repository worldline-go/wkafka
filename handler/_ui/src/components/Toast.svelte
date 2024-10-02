<script lang="ts">
  import { removeToast, storeToast } from "@/store/toast";
  import Icon from "@/components/Icon.svelte";

  const close = (id: number) => {
    removeToast(id);
  };
  const customSlide = (el: HTMLElement, { duration }: { duration: number }) => {
    return {
      duration,
      css: (_: any, u: any) => `transform: translateX(${u * 400}px)`,
    };
  };
</script>

<div class="fixed bottom-0 right-0 z-50">
  {#each $storeToast as toast (toast.id)}
    <div
      class={`${toast.type} flex p-2 h-12 items-center border-l border-t border-gray-700 w-[28rem]`}
      transition:customSlide={{ duration: 250 }}
    >
      <button on:click={() => close(toast.id)} class="hover:fill-red-500">
        <Icon icon="right" />
      </button>
      <div class="pl-2">
        <span>{toast.message}</span>
      </div>
    </div>
  {/each}
</div>

<style lang="scss">
  .alert {
    background-color: #f8d7da;
    color: #721c24;
  }

  .info {
    background-color: #d1ecf1;
    color: #0c5460;
  }

  .warn {
    background-color: #fff3cd;
    color: #856404;
  }
</style>
