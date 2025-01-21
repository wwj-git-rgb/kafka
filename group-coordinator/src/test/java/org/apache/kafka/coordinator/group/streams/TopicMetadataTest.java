/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.generated.StreamsGroupPartitionMetadataValue;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TopicMetadataTest {

    @Test
    public void testConstructor() {
        assertDoesNotThrow(() ->
            new TopicMetadata(Uuid.randomUuid(), "valid-topic", 3, new HashMap<>()));
    }

    @Test
    public void testConstructorWithZeroUuid() {
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
            new TopicMetadata(Uuid.ZERO_UUID, "valid-topic", 3, new HashMap<>()));
        assertEquals("Topic id cannot be ZERO_UUID.", exception.getMessage());
    }

    @Test
    public void testConstructorWithNullUuid() {
        assertThrows(NullPointerException.class, () ->
            new TopicMetadata(null, "valid-topic", 3, new HashMap<>()));
    }

    @Test
    public void testConstructorWithNullName() {
        assertThrows(NullPointerException.class, () ->
            new TopicMetadata(Uuid.randomUuid(), null, 3, new HashMap<>()));
    }

    @Test
    public void testConstructorWithEmptyName() {
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
            new TopicMetadata(Uuid.randomUuid(), "", 3, new HashMap<>()));
        assertEquals("Topic name cannot be empty.", exception.getMessage());
    }

    @Test
    public void testConstructorWithZeroNumPartitions() {
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
            new TopicMetadata(Uuid.randomUuid(), "valid-topic", 0, new HashMap<>()));
        assertEquals("Number of partitions must be positive.", exception.getMessage());
    }

    @Test
    public void testConstructorWithNegativeNumPartitions() {
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
            new TopicMetadata(Uuid.randomUuid(), "valid-topic", -1, new HashMap<>()));
        assertEquals("Number of partitions must be positive.", exception.getMessage());
    }

    @Test
    public void testConstructorWithNullPartitionRacks() {
        assertThrows(NullPointerException.class, () ->
            new TopicMetadata(Uuid.randomUuid(), "valid-topic", 3, null));
    }

    @Test
    public void testFromRecord() {
        StreamsGroupPartitionMetadataValue.TopicMetadata record = new StreamsGroupPartitionMetadataValue.TopicMetadata()
            .setTopicId(Uuid.randomUuid())
            .setTopicName("test-topic")
            .setNumPartitions(3)
            .setPartitionMetadata(List.of(
                new StreamsGroupPartitionMetadataValue.PartitionMetadata()
                    .setPartition(0)
                    .setRacks(List.of("rack1", "rack2")),
                new StreamsGroupPartitionMetadataValue.PartitionMetadata()
                    .setPartition(1)
                    .setRacks(List.of("rack3")),
                new StreamsGroupPartitionMetadataValue.PartitionMetadata()
                    .setPartition(2)
                    .setRacks(List.of("rack4", "rack5"))
            ));

        TopicMetadata topicMetadata = TopicMetadata.fromRecord(record);

        assertEquals(record.topicId(), topicMetadata.id());
        assertEquals(record.topicName(), topicMetadata.name());
        assertEquals(record.numPartitions(), topicMetadata.numPartitions());

        Map<Integer, Set<String>> expectedPartitionRacks = new HashMap<>();
        expectedPartitionRacks.put(0, Set.of("rack1", "rack2"));
        expectedPartitionRacks.put(1, Set.of("rack3"));
        expectedPartitionRacks.put(2, Set.of("rack4", "rack5"));

        assertEquals(expectedPartitionRacks, topicMetadata.partitionRacks());
    }
}