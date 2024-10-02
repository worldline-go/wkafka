export const formToObject = (form: HTMLFormElement) => {
  const formData = new FormData(form);
  const data: Record<string, any> = {};
  for (const field of formData) {
    const [key, value] = field;
    data[key] = value;
  }

  return data;
};
