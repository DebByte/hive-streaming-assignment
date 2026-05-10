// Telemetric data contracts

export type EventType =
  | "buffering_start"
  | "buffering_end"
  | "quality_change";

interface BaseTelemetryEvent {
    schemaVersion:      "1.0";
    eventType:          EventType;
    clientId:           string;
    contentId:          string;
    customerId:         string;
    timestamp:          number; 
    sessionDurationMs:  number;
}

//Fired when video.waiting event fires on the player.
export interface BufferingStartEvent extends BaseTelemetryEvent {
    eventType: "buffering_start";
}

//Fired when video.playing fires a buffering event on the player.
export interface BufferingEndEvent extends BaseTelemetryEvent {
    eventType:  "buffering_end";
    durationMs: number;
}

//Fired when there is a change in video quality, either due to network conditions or user action
export interface QualityChangeEvent extends BaseTelemetryEvent {
    eventType:      "quality_change";
    fromQuality:    string;
    toQuality:      string;
    bitrate:        number;
    width:          number;
    height:         number;
}

//Handle events generically as TelemetryEvent type
export type TelemetryEvent = BufferingStartEvent | BufferingEndEvent | QualityChangeEvent;

// Configuration for the collector
export interface CollectorConfig {
    clientId:           string;
    contentId:          string;
    customerId:         string;
    endpointUrl:        string;
    batchIntervalMs:    number;
    samplingRate:       number;
}





