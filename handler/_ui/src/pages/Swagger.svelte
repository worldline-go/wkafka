<script lang="ts">
  import axios, { AxiosError } from "axios";
  import SwaggerUI from "swagger-ui";
  import "swagger-ui/dist/swagger-ui.css";
  import update from "immutability-helper";
  import { onMount } from "svelte";

  let swaggerNode: HTMLDivElement;
  let msg = "";
  let isErr = false;

  let swag = {
    link: "../files/swagger.json",
    schemes: location.protocol,
    host: "",
    base_path: "",
    base_path_prefix: location.pathname.replace("/wkafka/ui/", ""),
    disable_authorize_button: true,
  };

  const disableAuthorizeButton = function () {
    return {
      wrapComponents: {
        authorizeBtn: () => () => null,
      },
    };
  };

  const setMsg = (m: string, err: boolean) => {
    msg = m;
    isErr = err;
  };

  const clearMsg = () => {
    msg = "";
    isErr = false;
  };

  onMount(() => {
    swaggerGet();
  });

  const swaggerGet = async () => {
    setMsg(`Loading...`, false);

    try {
      const response = await axios.get(swag.link);
      let swaggerData = response.data;
      // console.log(swaggerData);

      if (swag.schemes) {
        swaggerData = update(swaggerData, {
          schemes: { $set: swag.schemes },
        });
      }

      if (swag.host) {
        swaggerData = update(swaggerData, {
          host: { $set: swag.host },
        });
      }

      if (!!swag.base_path) {
        swaggerData = update(swaggerData, {
          basePath: { $set: swag.base_path },
        });
      }

      if (!!swag.base_path_prefix) {
        swaggerData = update(swaggerData, {
          basePath: { $set: `${swag.base_path_prefix}${swaggerData.basePath}` },
        });
      }

      let plugins = [];
      if (swag.disable_authorize_button) {
        plugins.push(disableAuthorizeButton);
      }

      SwaggerUI({
        domNode: swaggerNode,
        spec: swaggerData,
        plugins: plugins,
      });

      clearMsg();
    } catch (err) {
      setMsg((err as AxiosError).message, true);
    }
  };
</script>

<div class={!!msg ? "" : "hidden"}>
  <p class={`block p-2 text-white ${isErr ? "bg-red-500" : "bg-green-500"}`}>
    {msg}
  </p>
</div>

<div bind:this={swaggerNode} class={!!msg ? "hidden" : ""} />
