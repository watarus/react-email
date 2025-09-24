import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import { readStream } from './read-stream';

describe('readStream', () => {
  describe('multi-byte character handling', () => {
    it('should correctly decode Japanese characters split across chunks', async () => {
      // Create a string with Japanese text that will be split across chunks
      const japaneseText = 'これはテストです。スケジュール確認をお願いします。';
      const buffer = Buffer.from(japaneseText, 'utf-8');
      
      // Split the buffer at a position that would break a multi-byte character
      // "ス" in UTF-8 is E3 82 B9, let's split after E3 82
      const splitPosition = buffer.indexOf(Buffer.from('スケジュール')) + 2;
      
      // Create chunks that split the character
      const chunk1 = buffer.slice(0, splitPosition);
      const chunk2 = buffer.slice(splitPosition);
      
      // Create a mock PipeableStream that emits our chunks
      const mockStream = new Readable({
        read() {
          if (this.chunkIndex === 0) {
            this.push(chunk1);
            this.chunkIndex++;
          } else if (this.chunkIndex === 1) {
            this.push(chunk2);
            this.chunkIndex++;
          } else {
            this.push(null); // End the stream
          }
        }
      });
      mockStream.chunkIndex = 0;
      
      // Add pipe method to match PipeableStream interface
      const pipeableStream = {
        pipe: (writable: any) => mockStream.pipe(writable)
      };
      
      const result = await readStream(pipeableStream as any);
      
      // The result should match the original text without corruption
      expect(result).toBe(japaneseText);
      expect(result).not.toContain('\0'); // No null characters
      expect(result).toContain('スケジュール'); // Full word intact
    });

    it('should handle Chinese characters split across chunks', async () => {
      const chineseText = '这是一个测试。请确认您的日程安排。';
      const buffer = Buffer.from(chineseText, 'utf-8');
      
      // Split in the middle of a Chinese character
      const splitPosition = buffer.indexOf(Buffer.from('日程')) + 1;
      
      const chunk1 = buffer.slice(0, splitPosition);
      const chunk2 = buffer.slice(splitPosition);
      
      const mockStream = new Readable({
        read() {
          if (this.chunkIndex === 0) {
            this.push(chunk1);
            this.chunkIndex++;
          } else if (this.chunkIndex === 1) {
            this.push(chunk2);
            this.chunkIndex++;
          } else {
            this.push(null);
          }
        }
      });
      mockStream.chunkIndex = 0;
      
      const pipeableStream = {
        pipe: (writable: any) => mockStream.pipe(writable)
      };
      
      const result = await readStream(pipeableStream as any);
      
      expect(result).toBe(chineseText);
      expect(result).not.toContain('\0');
      expect(result).toContain('日程');
    });

    it('should handle emoji characters split across chunks', async () => {
      const emojiText = 'Hello 👋 World 🌍 Test 🚀';
      const buffer = Buffer.from(emojiText, 'utf-8');
      
      // Emojis are 4-byte UTF-8 sequences, split one
      const rocketEmoji = Buffer.from('🚀');
      const splitPosition = buffer.indexOf(rocketEmoji) + 2; // Split in middle of rocket emoji
      
      const chunk1 = buffer.slice(0, splitPosition);
      const chunk2 = buffer.slice(splitPosition);
      
      const mockStream = new Readable({
        read() {
          if (this.chunkIndex === 0) {
            this.push(chunk1);
            this.chunkIndex++;
          } else if (this.chunkIndex === 1) {
            this.push(chunk2);
            this.chunkIndex++;
          } else {
            this.push(null);
          }
        }
      });
      mockStream.chunkIndex = 0;
      
      const pipeableStream = {
        pipe: (writable: any) => mockStream.pipe(writable)
      };
      
      const result = await readStream(pipeableStream as any);
      
      expect(result).toBe(emojiText);
      expect(result).not.toContain('\0');
      expect(result).toContain('🚀');
    });

    it('should handle many small chunks with multi-byte characters', async () => {
      const mixedText = 'Test テスト 测试 Тест מבחן';
      const buffer = Buffer.from(mixedText, 'utf-8');
      
      // Create many small chunks (3 bytes each)
      const chunks: Buffer[] = [];
      for (let i = 0; i < buffer.length; i += 3) {
        chunks.push(buffer.slice(i, Math.min(i + 3, buffer.length)));
      }
      
      let currentChunk = 0;
      const mockStream = new Readable({
        read() {
          if (currentChunk < chunks.length) {
            this.push(chunks[currentChunk]);
            currentChunk++;
          } else {
            this.push(null);
          }
        }
      });
      
      const pipeableStream = {
        pipe: (writable: any) => mockStream.pipe(writable)
      };
      
      const result = await readStream(pipeableStream as any);
      
      expect(result).toBe(mixedText);
      expect(result).not.toContain('\0');
      expect(result).toContain('テスト');
      expect(result).toContain('测试');
      expect(result).toContain('Тест');
      expect(result).toContain('מבחן');
    });
  });

  describe('ReadableStream (pipeTo) path', () => {
    it('should handle multi-byte characters with ReadableStream', async () => {
      const japaneseText = 'バクラクのメールテンプレートでスケジュール確認';
      const buffer = Buffer.from(japaneseText, 'utf-8');
      
      // Split at a position that breaks a character
      const splitPosition = buffer.indexOf(Buffer.from('スケジュール')) + 2;
      const chunk1 = buffer.slice(0, splitPosition);
      const chunk2 = buffer.slice(splitPosition);
      
      // Mock a ReactDOMServerReadableStream
      const chunks = [chunk1, chunk2];
      let chunkIndex = 0;
      
      const mockReadableStream = {
        pipeTo: async (writable: WritableStream) => {
          const writer = writable.getWriter();
          
          for (const chunk of chunks) {
            await writer.write(chunk);
          }
          
          await writer.close();
        }
      };
      
      const result = await readStream(mockReadableStream as any);
      
      expect(result).toBe(japaneseText);
      expect(result).not.toContain('\0');
      expect(result).toContain('スケジュール');
    });
  });
});