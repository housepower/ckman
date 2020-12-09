export const Schame = {
  title: 'LoaderConfig',
  format: 'categories',
  basicCategoryTitle: 'Main',
  type: 'object',
  required: [
    'Common',
    'Kafka',
    'Tasks',
  ],
  properties: {
    Common: {
      type: 'object',
      title: 'Common',
      properties: {
        FlushInterval: {
          $ref: '#/definitions/flushInterval',
        },
        BufferSize: {
          $ref: '#/definitions/bufferSize',
        },
        MinBufferSize: {
          $ref: '#/definitions/minBufferSize',
        },
        MsgSizeHint: {
          $ref: '#/definitions/msgSizeHint',
        },
        LayoutDate: {
          $ref: '#/definitions/layoutDate',
        },
        LayoutDateTime: {
          $ref: '#/definitions/layoutDateTime',
        },
        LayoutDateTime64: {
          $ref: '#/definitions/layoutDateTime64',
        },
        Replicas: {
          $ref: '#/definitions/replicas',
        },
        LogLevel: {
          type: 'string',
          enum: [
            'panic',
            'fatal',
            'error',
            'warn',
            'info',
            'debug',
            'trace',
          ],
          default: 'info',
        },
      },
    },
    Kafka: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          Name: {
            type: 'string',
          },
          Brokers: {
            type: 'string',
          },
          Version: {
            type: 'string',
          },
          TLS: {
            type: 'object',
            properties: {
              Enable: {
                type: 'boolean',
                default: false,
              },
              CaCertFiles: {
                type: 'string',
              },
              ClientCertFile: {
                type: 'string',
              },
              ClientKeyFile: {
                type: 'string',
              },
              InsecureSkipVerify: {
                type: 'boolean',
                default: false,
              },
            },
          },
          Sasl: {
            type: 'object',
            properties: {
              Enable: {
                type: 'boolean',
                default: false,
              },
              Mechanism: {
                type: 'string',
                enum: [
                  'PLAIN',
                  'SCRAM-SHA-256',
                  'SCRAM-SHA-512',
                  'GSSAPI',
                ],
                default: 'PLAIN',
              },
              Username: {
                type: 'string',
              },
              Password: {
                type: 'string',
              },
              GSSAPI: {
                type: 'object',
                properties: {
                  AuthType: {
                    type: 'integer',
                    enum: [
                      1,
                      2,
                    ],
                    default: 2,
                  },
                  KeyTabPath: {
                    type: 'string',
                  },
                  KerberosConfigPath: {
                    type: 'string',
                  },
                  ServiceName: {
                    type: 'string',
                  },
                  Username: {
                    type: 'string',
                  },
                  Password: {
                    type: 'string',
                  },
                  Realm: {
                    type: 'string',
                  },
                  DisablePAFXFAST: {
                    type: 'boolean',
                  },
                },
              },
            },
          },
        },
        defaultProperties: [
          'Name',
          'Brokers',
          'Version',
        ],
      },
    },
    Tasks: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          Name: {
            type: 'string',
          },
          KafkaClient: {
            type: 'string',
            enum: [
              'kafka-go',
              'sarama',
            ],
            default: 'kafka-go',
          },
          Kafka: {
            type: 'string',
          },
          Topic: {
            type: 'string',
          },
          ConsumerGroup: {
            type: 'string',
          },
          Earliest: {
            type: 'boolean',
          },
          Parser: {
            type: 'string',
            enum: [
              'fastjson',
              'gjson',
              'csv',
            ],
            default: 'fastjson',
          },
          CsvFormat: {
            type: 'array',
            format: 'table',
            items: {
              type: 'string',
            },
          },
          Delimiter: {
            type: 'string',
          },
          Clickhouse: {
            type: 'string',
          },
          TableName: {
            type: 'string',
          },
          AutoSchema: {
            type: 'boolean',
            default: true,
          },
          ExcludeColumns: {
            type: 'array',
            format: 'table',
            items: {
              type: 'string',
            },
          },
          Dims: {
            type: 'array',
            format: 'table',
            items: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                },
                type: {
                  type: 'string',
                },
                sourceName: {
                  type: 'string',
                },
              },
            },
          },
          ShardingKey: {
            type: 'string',
          },
          ShardingPolicy: {
            type: 'string',
          },
          FlushInterval: {
            $ref: '#/definitions/flushInterval',
          },
          BufferSize: {
            $ref: '#/definitions/bufferSize',
          },
          MinBufferSize: {
            $ref: '#/definitions/minBufferSize',
          },
          MsgSizeHint: {
            $ref: '#/definitions/msgSizeHint',
          },
          LayoutDate: {
            $ref: '#/definitions/layoutDate',
          },
          LayoutDateTime: {
            $ref: '#/definitions/layoutDateTime',
          },
          LayoutDateTime64: {
            $ref: '#/definitions/layoutDateTime64',
          },
          Replicas: {
            $ref: '#/definitions/replicas',
          },
        },
        defaultProperties: [
          'Name',
          'Kafka',
          'Topic',
          'ConsumerGroup',
          'Parser',
          'Clickhouse',
          'TableName',
        ],
      },
    },
  },
  definitions: {
    flushInterval: {
      type: 'integer',
      default: 3,
    },
    bufferSize: {
      type: 'integer',
      default: 1048576,
    },
    minBufferSize: {
      type: 'integer',
      default: 8196,
    },
    msgSizeHint: {
      type: 'integer',
      default: 1000,
    },
    layoutDate: {
      type: 'string',
      enum: [
        '2006-01-02',
        '01/02/2006',
      ],
      default: '2006-01-02',
    },
    layoutDateTime: {
      type: 'string',
      enum: [
        '2006-01-02T15:04:05+07:00',
        '2006-01-02 15:04:05',
      ],
      default: '2006-01-02T15:04:05+07:00',
    },
    layoutDateTime64: {
      type: 'string',
      enum: [
        '2006-01-02T15:04:05+07:00',
        '2006-01-02 15:04:05.999999999',
      ],
      default: '2006-01-02T15:04:05+07:00',
    },
    replicas: {
      type: 'integer',
      default: 1,
    },
  },
};
