// Polyfill
if (!Object.entries) {
  Object.entries = function (obj) {
    let ownProps = Object.keys(obj), i = ownProps.length, resArray = new Array(i); // preallocate the Array
    while (i--) resArray[i] = [ownProps[i], obj[ownProps[i]]];
    return resArray;
  };
}
if (!Object.values) {
  Object.values = function (obj) {
    if (obj !== Object(obj)) throw new TypeError('Object.values called on a non-object');
    let val = [], key;
    for (key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) val.push(obj[key]);
    }
    return val;
  }
}

/////////////////////////////////////////

/** @type {Worker} */
const ctx = self;

const detailsField = '@details';
const messageField = '@message';

const events = new class WorkerEventHandler {
  /** @type {object[]} */
  plainData = null;
  trueTotal = 0;
  id = undefined;

  async initData(params) {
    const { id, mapping, tags, splResult } = params
    this.plainData = null;
    this.id = id;
    Object.entries(mapping).forEach(([key, type]) => {
      mapping[key] = {
        type,
        total: 0,
        map: new Map(),
        min: type === 'number' ? Infinity : undefined,
        max: type === 'number' ? -Infinity : undefined,
      };
    });

    mapping['@tags'] = {
      type: 'array',
      total: 0,
      map: new Map(tags.map(x => [x.tagName, 0])),
    };

    try {
      if (splResult.type !== 'events') throw 'aggregation';

      this.plainData = splResult.events.events;
      this.trueTotal = splResult.events.total;

      this.plainData.forEach(record => {
        record.entries = Object.entries(record.source).sort((a, b) => a[0].localeCompare(b[0]));
        if (Array.isArray(record.source[detailsField])) {
          record.__message = record.source[detailsField].map(x => x[messageField]).join('\n');
        } else {
          record.__message = record.source[messageField];
        }
        record.__id = `${record._index}-${record._type}-${record._id}`;
      });
      return this.process(mapping);
    } catch (e) {
      this.postProcess(mapping, 0);
      throw e;
    }
  }

  getPagedData(params) {
    const { from, size, sort, asc } = params;
    let data = this.plainData || [];
    if (sort) {
      data = data.slice().sort(asc
        ? (a, b) => a.source[sort] < b.source[sort] ? -1 : +(a.source[sort] !== b.source[sort])
        : (a, b) => a.source[sort] > b.source[sort] ? -1 : +(a.source[sort] !== b.source[sort])
      );
    }
    return {
      data: data.slice(from, from + size),
      total: data.length,
      trueTotal: this.trueTotal,
    };
  }

  /**
   * @param {{ [key: string]: { total: number, map: Map<any, number> } }} fields
   * @param {number} count
   * @param {string} type
   */
  postProcess(fields, count) {
    const result = {
      user: [],
      system: [],
      tags: [],
      id: this.id,
    };
    for (const [name, agg] of Object.entries(fields)) {
      const entries = Array.from(agg.map.entries());
      if (!(entries.length >= count && entries.length === agg.total)) {
        entries.sort((a, b) => b[1] - a[1]);
      }
      const top5Data = agg.type === 'object' ? [] : entries.slice(0, 5);
      // eslint-disable-next-line no-nested-ternary
      result[name.startsWith('@') ? (name === '@tags' ? 'tags' : 'system') : 'user'].push({
        name,
        type: agg.type, // 值类型
        data: agg.type === 'object' ? [] : entries,
        count, // 统计的记录数
        hasValueCount: agg.total, // 共多少字段有值
        valueTypeCount: entries.length, // 共有多少种值
        top5Data, // 前5种值
        top5Count: top5Data.reduce((prev, curr) => prev + curr[1], 0), // 前5种值的数量和
        selected: name !== detailsField && name !== messageField,
        min: agg.min,
        max: agg.max,
      });
    }
    Object.values(result).forEach(x => {
      if (!Array.isArray(x)) return;
      x.sort((a, b) => a.name.localeCompare(b.name));
    });
    if (result.tags[0]) {
      result.tags[0].data.sort((a, b) => a[0].localeCompare(b[0]));
    }

    return result;
  }

  /** @param {{ [key: string]: { total: number, map: Map<any, number> } }} fields*/
  process(fields) {
    for (const event of this.plainData) {
      for (let [key, value] of Object.entries(event.source)) {
        let type = typeof value;
        if (type === 'object') {
          if (Array.isArray(value)) {
            type = 'array';
          } else {
            value = null;
          }
        }
        /** @type {{ total: number, map: Map<any, number>, min: any, max: any }} */
        let agg = fields[key];
        if (!agg) {
          agg = fields[key] = {
            total: 0,
            type,
            map: new Map(),
            min: type === 'number' ? value : undefined,
            max: type === 'number' ? value : undefined,
          };
        }
        ++agg.total;
        if (key !== '@message') {
          if (type === 'array') {
            value.forEach(x => {
              agg.map.set(x, (agg.map.get(x) || 0) + 1);
            });
          } else {
            agg.map.set(value, (agg.map.get(value) || 0) + 1);
            if (type === 'number') {
              agg.min = Math.min(agg.min, value);
              agg.max = Math.max(agg.max, value);
            }
          }
        }
      }
    }
    return this.postProcess(fields, this.plainData.length);
  }
};

ctx.onmessage = async msg => {
  try {
    ctx.postMessage(['resolve', await events[msg.data.type](msg.data.args)]);
  } catch (e) {
    ctx.postMessage(['reject', e]);
  }
};
