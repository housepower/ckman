import moment from 'moment';

/**
 * 转换时间段字符串至数字（经过的毫秒数）
 * duration 时间段字符串
 */
export function parseDuration(duration: string | number) {
  if (typeof duration === 'string') {
    const [ , value, unit ] = /^(\d+)([smhdwMy])$/.exec(duration);
    return moment.duration(+value, unit as moment.unitOfTime.DurationConstructor);
  } else {
    return +duration;
  }
}
