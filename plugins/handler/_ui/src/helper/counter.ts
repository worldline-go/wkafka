export let countDownTimer = (end: number, fn: (v: string) => void) => {
  let _second = 1000;
  let _minute = _second * 60;
  let _hour = _minute * 60;
  let timer: number;

  let showRemaining = () => {
    let distance = end - Date.now();
    if (distance < 0) {
      clearInterval(timer);

      fn('');

      return;
    }

    let hours = Math.floor(distance / _hour);
    let minutes = Math.floor((distance % _hour) / _minute);
    let seconds = Math.floor((distance % _minute) / _second);

    fn(hours + 'hrs ' + minutes + 'mins ' + seconds + 'secs');
  }

  timer = setInterval(showRemaining, 1000);
  return timer;
}
