import { storeInfo } from '@/store/store';
import type { Info } from '@/store/model';

const endpoints = {
  info: '../v1/info',
  skip: '../v1/skip',
  event: '../v1/event',
}

const eventInfoListener = () => {
  const event = new EventSource(endpoints.event);
  event.addEventListener('info', (e) => {
    let data = JSON.parse(e.data) as Info;
    storeInfo.set(data);
  });

  return event;
}

export { eventInfoListener };
