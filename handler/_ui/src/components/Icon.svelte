<script lang="ts">
  const icons = {
    link: "M14 4h-13v18h20v-11h1v12h-22v-20h14v1zm10 5h-1v-6.293l-11.646 11.647-.708-.708 11.647-11.646h-6.293v-1h8v8z",
    right:
      "M7.33 24l-2.83-2.829 9.339-9.175-9.339-9.167 2.83-2.829 12.17 11.996z",
    left: "M16.67 24l2.83-2.829-9.339-9.175 9.339-9.167-2.83-2.829-12.17 11.996z",
    menu: "M3 6h18v-2h-18v2zm0 6h18v-2h-18v2zm0 6h18v-2h-18v2z",
  };

  type IconTypesString = keyof typeof icons;
  export let icon: IconTypesString;

  export let height = "24";
  export let width = "24";

  let className: string | undefined = undefined;
  export { className as class };
</script>

<svg
  xmlns="http://www.w3.org/2000/svg"
  class={"inline " + className}
  {width}
  {height}
  viewBox="0 0 24 24"
  fill-rule="evenodd"
  clip-rule="evenodd"
>
  <path d={icons[icon]} />
</svg>
