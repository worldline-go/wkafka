import type { Header } from '@/store/model';
import { Base64 } from 'js-base64';

export const formToObject = (form: HTMLFormElement) => {
  const formData = new FormData(form);
  const data: Record<string, any> = {};
  for (const field of formData) {
    const [key, value] = field;
    data[key] = value;
  }

  return data;
};

export const base64ToStr = (v: string) => {
  try {
    return Base64.decode(v);
  } catch (error) {
    console.warn(error);
    return "";
  }
}

export const parseJSON = (v: string) => {
  try {
    return JSON.parse(v);
  } catch (error) {
    console.warn(error);
    return null;
  }
}

export const getField = (field: string, headers: Header[]) => {
  const header = headers.find(h => h.key === field);
  if (!header) {
    return '';
  }

  return header.value;
}

export const getFieldWithDecode = (field: string, headers: Header[]) => {
  return base64ToStr(getField(field, headers));
}
