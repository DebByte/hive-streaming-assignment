import Hls from "hls.js";
import {
    TelemetryEvent,
    BufferingStartEvent,
    BufferingEndEvent,
    QualityChangeEvent,
    CollectorConfig
} from "./types";

export class HiveStreamingCollector {
    private hls: Hls;
    private video: HTMLVideoElement;
    private config: CollectorConfig;
    private eventQueue: TelemetryEvent[] = [];
    private bufferingStartedAt: number | null = null;
    private currentQuality: string = "unknown";
    private sessionStartedAt: number = Date.now();
    private flushTimer: ReturnType<typeof setInterval> | null = null;

    constructor(video: HTMLVideoElement, hls: Hls, config: CollectorConfig) {
        this.hls = hls;
        this.video = video;
        this.config = {
            ...config,
            batchIntervalMs: config.batchIntervalMs ?? 10_000,
            samplingRate: config.samplingRate ?? 1.0
        };

        if (Math.random() > this.config.samplingRate) {
            console.debug("[HiveStreaming] Session excluded by sampling");
            return;
        }

        this.attach();
        this.startFlushTimer();
    }

    private attach(): void {
        // Native video element events for buffering detection
        this.video.addEventListener("waiting", this.onBufferingStart);
        this.video.addEventListener("playing", this.onBufferingEnd);
        this.video.addEventListener("ended", this.onBufferingEnd);

        // HLS.js event for quality level switches (ABR)
        this.hls.on(Hls.Events.LEVEL_SWITCHED, this.onQualityChange);

        // Flush remaining events before page unloads
        window.addEventListener("beforeunload", this.flush);
    }

    private onBufferingStart = (): void => {
        if (this.bufferingStartedAt !== null) return; // already buffering
        this.bufferingStartedAt = Date.now();

        this.enqueue<BufferingStartEvent>({
            schemaVersion: "1.0",
            eventType: "buffering_start",
            clientId: this.config.clientId,
            contentId: this.config.contentId,
            customerId: this.config.customerId,
            timestamp: Date.now(),
            sessionDurationMs: this.getSessionDuration()
        });
    }

    private onBufferingEnd = (): void => {
        if (this.bufferingStartedAt === null) return; // not buffering
        const durationMs = Date.now() - this.bufferingStartedAt;
        this.bufferingStartedAt = null;

        this.enqueue<BufferingEndEvent>({
            schemaVersion: "1.0",
            eventType: "buffering_end",
            clientId: this.config.clientId,
            contentId: this.config.contentId,
            customerId: this.config.customerId,
            timestamp: Date.now(),
            sessionDurationMs: this.getSessionDuration(),
            durationMs                             // how long it buffered
        });
    }


    private onQualityChange = (_: string, data: { level: number }): void => {
        const level = this.hls.levels[data.level];
        const toQuality = `${level.height}p`;
        const fromQuality = this.currentQuality;
        this.currentQuality = toQuality;

        this.enqueue<QualityChangeEvent>({
            schemaVersion: "1.0",
            eventType: "quality_change",
            clientId: this.config.clientId,
            contentId: this.config.contentId,
            customerId: this.config.customerId,
            timestamp: Date.now(),
            sessionDurationMs: this.getSessionDuration(),
            fromQuality,
            toQuality,
            bitrate: level.bitrate,
            width: level.width,
            height: level.height
        });
    }

    private enqueue<T extends TelemetryEvent>(event: T): void {
        this.eventQueue.push(event);
    }

    private startFlushTimer(): void {
        this.flushTimer = setInterval(
            this.flush,
            this.config.batchIntervalMs
        );
    }

    // Uses sendBeacon — guaranteed delivery even on page close
    private flush = (): void => {
        if (this.eventQueue.length === 0) return;

        const batch = [...this.eventQueue];
        this.eventQueue = [];

        const payload = JSON.stringify({
            clientId: this.config.clientId,
            contentId: this.config.contentId,
            customerId: this.config.customerId,
            events: batch,
            batchSize: batch.length,
            sentAt: Date.now()
        });

        // Falls back to fetch if sendBeacon not available
        if (navigator.sendBeacon) {
            navigator.sendBeacon(this.config.endpointUrl, payload);
        } else {
            fetch(this.config.endpointUrl, {
                method: "POST",
                body: payload,
                headers: { "Content-Type": "application/json" },
                keepalive: true
            }).catch(console.error);
        }
    }

    private getSessionDuration(): number {
        return Date.now() - this.sessionStartedAt;
    }

    //CleanUp
    public destroy(): void {
        this.flush();                          // send remaining events
        if (this.flushTimer) clearInterval(this.flushTimer);
        this.video.removeEventListener("waiting", this.onBufferingStart);
        this.video.removeEventListener("playing", this.onBufferingEnd);
        this.video.removeEventListener("ended", this.onBufferingEnd);
        this.hls.off(Hls.Events.LEVEL_SWITCHED, this.onQualityChange);
        window.removeEventListener("beforeunload", this.flush);
    }
}
