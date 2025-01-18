import { storeInfoAdd, storeInfoDelete, storeInfoClear } from '@/store/store';
import type { Info } from '@/store/model';
import { addToast } from '@/store/toast';
import axios from 'axios';

const endpoints = {
  info: '../v1/info',
  skip: '../v1/skip',
  retryDLQ: '../v1/retry-dlq',
  event: '../v1/event',
}

export const eventInfoListener = () => {
  const event = new EventSource(endpoints.event);
  event.addEventListener('info', (e) => {
    let data = JSON.parse(e.data) as Info;
    storeInfoAdd(data);
  });

  event.addEventListener('delete', (e) => {
    storeInfoDelete(e.data as string);
  });

  event.onopen = () => {
    console.log('event source connected');
    storeInfoClear();
  }

  return event;
}

export const skip = async (topic: string, partition: number, offset: number) => {
  // add promt to confirm
  let result = confirm(`Are you sure to skip message on
  topic: ${topic}
  partition: ${partition}
  offset: ${offset}`
  );

  if (!result) {
    return;
  }

  try {
    await axios.patch(endpoints.skip, {
      [topic]: {
        [partition.toString()]: {
          offsets: [offset],
        }
      }
    }, {
      headers: {
        'Content-Type': 'application/json'
      }
    });

    addToast('skip request sent');
  } catch (error) {
    console.error(error);
    addToast('skip request failed', 'alert');
  }
}

export const skipClear = async () => {
  try {
    await axios.put(endpoints.skip, {}, {
      headers: {
        'Content-Type': 'application/json'
      }
    });

    addToast('skip clear request sent');
  } catch (error) {
    console.error(error);
    addToast('skip clear request failed', 'alert');
  }
}

export const retry = async (topic: string, partition: number, offset: number) => {
  try {
    await axios.post(endpoints.retryDLQ, {
      "spec": {
        "topic": topic,
        "partition": partition,
        "offset": offset
      }
    }, {
      headers: {
        'Content-Type': 'application/json'
      }
    });

    addToast('retry request sent');
  } catch (error) {
    console.error(error);
    addToast('retry request failed', 'alert');
  }
}
