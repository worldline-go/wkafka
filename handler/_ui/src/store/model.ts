export interface Info {
  dlq_topics?: string[];
  topics?: string[];
  skip?: Map<string, Map<number, OffsetConfig>>;
  dlq_record?: DlqRecord;
}

export interface OffsetConfig {
  offsets: number[];
  before: number;
}

export interface DlqRecord {
  topic: string;
  partition: number;
  offset: number;
  key: string; // base64 encoded
  value: string; // base64 encoded
  timestamp: string;
  headers: Header[];
}

export interface Header {
  key: string;
  value: string; // base64 encoded
}
