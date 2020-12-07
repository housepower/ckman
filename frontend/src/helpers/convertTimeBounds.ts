import moment from 'moment';

import { TimeFilterType } from '@/models';

import { parseTime } from './parseTime';

export interface TimeBoundsModel {
  min: number;
  max: number;
  duration: moment.Duration;
}

export function convertTimeBounds(timeFilter: TimeFilterType): TimeBoundsModel {
  const [ start, end ] = timeFilter.map(parseTime);

  return {
    min: start.timestamp,
    max: end.timestamp,
    duration: moment.duration(end.timestamp - start.timestamp),
  };
}
