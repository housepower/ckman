import moment, { unitOfTime } from 'moment';

/**
 * 解析时刻字符串
 * time 所需解析的时刻（可以为相对时间或绝对时间）
 */
export function parseTime(time: string | number | Record<string, any>) {
  switch (typeof time) {
    case 'string': {
      const timeNumber = +time;
      if (!isNaN(timeNumber)) {
        return {
          type: 'absolute',
          value: timeNumber,
          moment: moment(timeNumber),
          timestamp: timeNumber,
        };
      }

      const matches = /^now(?:-((\d+)([smhdwMy])(?:\/([dwMy]))?))?$/.exec(time);
      if (matches) {
        const num = +matches[2] || 0;
        const unit = matches[3] || 'd';
        const trunc = matches[4];
        let momentObj = +moment.duration(num, unit as unitOfTime.DurationConstructor);
        let timestamp = new Date(Date.now() - momentObj).setMilliseconds(0);

        if (trunc) {
          timestamp = +moment(timestamp).startOf(trunc as unitOfTime.StartOf);
          momentObj = +moment.duration(new Date().setMilliseconds(0) - timestamp);
        }

        return {
          type: 'relative',
          value: matches[1] || '0d',
          number: num,
          unit,
          moment: momentObj,
          trunc,
          timestamp,
        };
      } else {
        const value = Date.parse(time);
        if (value) {
          return {
            type: 'absolute',
            value,
            moment: moment(value),
            timestamp: value,
          };
        }
      }

      throw new TypeError('不支持的时间类型');
    }

    case 'number':
    case 'object': {
      return {
        type: 'absolute',
        value: +time,
        timestamp: +time,
      };
    }

    default:
      throw new TypeError('不支持的时间类型');
  }
}
