export interface kafka {
  topics: string[];
  dlq: string;
  messages: kafkaMessage[];
};

export interface kafkaMessage {
  topic: string;
  message: any;
  offset: number;
  partition: number;

  headers: {
    key: string;
    value: string;
  }[];
};
