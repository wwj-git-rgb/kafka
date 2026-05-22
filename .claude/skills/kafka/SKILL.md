```markdown
# kafka Development Patterns

> Auto-generated skill from repository analysis

## Overview
This skill teaches the core development patterns and conventions used in the `kafka` Java codebase. You'll learn about file naming, import/export styles, commit message formatting, and how to write and organize tests. While no specific frameworks or automated workflows were detected, this guide will help you maintain consistency and quality in your contributions.

## Coding Conventions

### File Naming
- **Convention:** PascalCase
- **Example:**  
  ```java
  public class MessageProcessor { ... }
  ```

### Import Style
- **Convention:** Relative imports are used.
- **Example:**
  ```java
  import com.mycompany.kafka.utils.MessageUtils;
  ```

### Export Style
- **Convention:** Named exports (Java's `public` classes/methods).
- **Example:**
  ```java
  public class KafkaProducer { ... }
  ```

### Commit Message Patterns
- **Convention:** Ticket reference as prefix, with descriptive message.
- **Example:**  
  ```
  KAFKA-1234: Fix producer retry logic for idempotence
  ```

## Workflows

### Commit with Ticket Reference
**Trigger:** When committing code changes.
**Command:** `/commit-ticket`

1. Start your commit message with the ticket reference (e.g., `KAFKA-1234:`).
2. Write a concise, descriptive message (average length ~86 characters).
3. Example:
   ```
   KAFKA-5678: Update consumer group balancing algorithm
   ```

## Testing Patterns

- **Test File Pattern:** `*.test.ts` (Note: This suggests some TypeScript tests may exist, even though the main codebase is Java.)
- **Framework:** Unknown (no specific testing framework detected).
- **Convention:** Place test files alongside or in a dedicated test directory, using the `.test.ts` suffix.
- **Example:**
  ```typescript
  // MessageProcessor.test.ts
  import { MessageProcessor } from '../src/MessageProcessor';

  describe('MessageProcessor', () => {
    it('should process messages correctly', () => {
      // test implementation
    });
  });
  ```

## Commands
| Command         | Purpose                                                |
|-----------------|--------------------------------------------------------|
| /commit-ticket  | Format commit message with ticket reference prefix      |
```
