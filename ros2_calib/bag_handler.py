# MIT License
#
# Copyright (c) 2025 Institute for Automotive Engineering (ika), RWTH Aachen University
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from pathlib import Path

from PySide6.QtCore import QThread, Signal
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore


def get_topic_info(bag_file):
    """Get topic information from bag file as list of tuples (topic_name, topic_type, message_count)."""
    topics = []
    typestore = get_typestore(Stores.ROS2_JAZZY)  # Or another appropriate ROS2 version
    bag_path = Path(bag_file)
    with AnyReader([bag_path.parent], default_typestore=typestore) as reader:
        for connection in reader.connections:
            topics.append((connection.topic, connection.msgtype, connection.msgcount))
    return topics


def get_total_message_count(bag_file):
    """Get total message count from bag file for progress tracking."""
    typestore = get_typestore(Stores.ROS2_JAZZY)
    bag_path = Path(bag_file)
    total_count = 0
    with AnyReader([bag_path.parent], default_typestore=typestore) as reader:
        for connection in reader.connections:
            total_count += connection.msgcount
    return total_count


def read_messages(bag_file, topics_to_read):
    """Read specific messages from bag file.

    Args:
        bag_file: Path to bag file
        topics_to_read: Either dict with topic names as keys, or list of topic names

    Returns:
        Generator yielding (timestamp, message) tuples
    """
    if isinstance(topics_to_read, dict):
        topics_needed = set(topics_to_read.keys())
    else:
        topics_needed = set(topics_to_read)

    typestore = get_typestore(Stores.ROS2_JAZZY)
    bag_path = Path(bag_file)
    with AnyReader([bag_path.parent], default_typestore=typestore) as reader:
        for connection, timestamp, rawdata in reader.messages():
            if connection.topic in topics_needed:
                msg = reader.deserialize(rawdata, connection.msgtype)
                yield timestamp, msg


def read_single_messages(bag_file, topics_to_read):
    """Read single message per topic from bag file (legacy function).

    Args:
        bag_file: Path to bag file
        topics_to_read: Dict with topic names as keys

    Returns:
        Dict mapping topic names to messages
    """
    messages = {}
    topics_needed = set(topics_to_read.keys())
    typestore = get_typestore(Stores.ROS2_JAZZY)
    bag_path = Path(bag_file)
    with AnyReader([bag_path.parent], default_typestore=typestore) as reader:
        for connection, timestamp, rawdata in reader.messages():
            if connection.topic in topics_needed:
                msg = reader.deserialize(rawdata, connection.msgtype)
                messages[connection.topic] = msg
                topics_needed.remove(connection.topic)
                if not topics_needed:
                    break
    return messages


def iterate_all_messages(bag_file: str):
    """Generator to iterate through all messages in the rosbag efficiently."""
    typestore = get_typestore(Stores.ROS2_JAZZY)
    bag_path = Path(bag_file)

    with AnyReader([bag_path.parent], default_typestore=typestore) as reader:
        for connection, timestamp, rawdata in reader.messages():
            msg = reader.deserialize(rawdata, connection.msgtype)
            yield timestamp, connection.topic, msg


def combine_tf_static_messages(tf_messages):
    """Combine multiple tf_static messages into one composite message."""
    if not tf_messages:
        return None

    if len(tf_messages) == 1:
        return tf_messages[0]

    # Use the first message as base and add transforms from others
    base_message = tf_messages[0]
    all_transforms = list(base_message.transforms) if hasattr(base_message, "transforms") else []

    # Add transforms from other messages, avoiding duplicates
    seen_transforms = set()
    for transform in all_transforms:
        key = (transform.header.frame_id, transform.child_frame_id)
        seen_transforms.add(key)

    for msg in tf_messages[1:]:
        if hasattr(msg, "transforms"):
            for transform in msg.transforms:
                key = (transform.header.frame_id, transform.child_frame_id)
                if key not in seen_transforms:
                    all_transforms.append(transform)
                    seen_transforms.add(key)

    # Create a new message with all transforms
    combined_message = base_message
    if hasattr(combined_message, "transforms"):
        combined_message.transforms = all_transforms

    return combined_message


def read_all_messages_optimized(
    bag_file: str,
    topics_to_read: dict,
    progress_callback=None,
    total_messages=None,
    frame_samples: int = 6,
    topic_message_counts=None,
) -> dict:
    """Read multiple uniformly sampled frame messages from rosbag."""
    # Separate sensor topics from tf_static topics
    sensor_topics = {
        topic: msg_type for topic, msg_type in topics_to_read.items() if "tf_static" not in topic
    }

    if topic_message_counts is None:
        # Fallback: count messages (should not happen in normal operation)
        topic_counts = {topic: 0 for topic in sensor_topics.keys()}
        if progress_callback:
            progress_callback.emit(10, "Counting messages for sampling...")

        for timestamp, topic, msg_data in iterate_all_messages(bag_file):
            if topic in topic_counts:
                topic_counts[topic] += 1
    else:
        # Use the provided counts
        topic_counts = {topic: topic_message_counts.get(topic, 0) for topic in sensor_topics.keys()}
        if progress_callback:
            progress_callback.emit(15, "Using existing topic counts for sampling...")

    # Calculate sampling intervals
    sampling_intervals = {}
    for topic, count in topic_counts.items():
        if count > 0:
            sampling_intervals[topic] = max(1, count // frame_samples)
            if progress_callback:
                progress_callback.emit(
                    20,
                    f"Topic {topic}: {count} messages, sampling every {sampling_intervals[topic]}",
                )

    # Collect sampled messages and tf_static
    messages = {}
    tf_static_messages = []
    topic_message_counts = {topic: 0 for topic in sensor_topics.keys()}
    collected_samples = {topic: [] for topic in sensor_topics.keys()}

    processed_count = 0
    for timestamp, topic, msg_data in iterate_all_messages(bag_file):
        processed_count += 1

        # Update progress if callback provided
        if progress_callback and total_messages and processed_count % 100 == 0:
            progress = int((processed_count / total_messages) * 70) + 20  # Scale to 20-90%
            progress_callback.emit(
                min(progress, 90), f"Sampling frames {processed_count}/{total_messages}..."
            )

        if topic in topics_to_read:
            # For tf_static, collect ALL messages
            if "tf_static" in topic:
                tf_static_messages.append(msg_data)
            # For sensor topics, collect samples uniformly
            elif topic in sensor_topics:
                topic_message_counts[topic] += 1
                interval = sampling_intervals.get(topic, 1)

                # Sample at regular intervals
                if (topic_message_counts[topic] - 1) % interval == 0 and len(
                    collected_samples[topic]
                ) < frame_samples:
                    collected_samples[topic].append(
                        {
                            "timestamp": timestamp,
                            "data": msg_data,
                            "topic_type": topics_to_read[topic],
                        }
                    )

    # Store collected samples
    messages["frame_samples"] = collected_samples

    # Combine all tf_static messages
    for topic in topics_to_read:
        if "tf_static" in topic and tf_static_messages:
            messages[topic] = combine_tf_static_messages(tf_static_messages)

    if progress_callback:
        total_collected = sum(len(samples) for samples in collected_samples.values())
        progress_callback.emit(90, f"Collected {total_collected} frame samples total")

    return messages


def convert_to_mock(raw_msg, msg_type):
    """Convert raw rosbag message to mock ROS message objects."""
    from . import ros_utils

    if msg_type == "sensor_msgs/msg/Image":
        return ros_utils.Image(
            header=raw_msg.header,
            height=raw_msg.height,
            width=raw_msg.width,
            encoding=raw_msg.encoding,
            is_bigendian=raw_msg.is_bigendian,
            step=raw_msg.step,
            data=raw_msg.data,
        )
    if msg_type == "sensor_msgs/msg/CompressedImage":
        # We can treat it similarly to Image for our purposes, but the data is different
        # The calibration widget will handle the decompression.
        mock_img = ros_utils.Image(
            header=raw_msg.header,
            encoding=raw_msg.format,  # CompressedImage uses 'format'
            data=raw_msg.data,
        )
        mock_img._type = "sensor_msgs/msg/CompressedImage"  # Add a temporary attribute
        return mock_img
    elif msg_type == "sensor_msgs/msg/PointCloud2":
        fields = []
        for f in raw_msg.fields:
            fields.append(
                ros_utils.PointField(
                    name=f.name, offset=f.offset, datatype=f.datatype, count=f.count
                )
            )
        return ros_utils.PointCloud2(
            header=raw_msg.header,
            height=raw_msg.height,
            width=raw_msg.width,
            fields=fields,
            is_bigendian=raw_msg.is_bigendian,
            point_step=raw_msg.point_step,
            row_step=raw_msg.row_step,
            data=raw_msg.data,
            is_dense=raw_msg.is_dense,
        )
    elif msg_type == "sensor_msgs/msg/CameraInfo":
        return ros_utils.CameraInfo(
            header=raw_msg.header,
            height=raw_msg.height,
            width=raw_msg.width,
            distortion_model=raw_msg.distortion_model,
            d=raw_msg.d,
            k=raw_msg.k,
            r=raw_msg.r,
            p=raw_msg.p,
        )
    return raw_msg


class RosbagProcessingWorker(QThread):
    """Worker thread for processing rosbag data without blocking the UI."""

    progress_updated = Signal(int, str)
    processing_finished = Signal(dict, dict, dict)  # raw_messages, topic_types, selected_topics
    processing_failed = Signal(str)  # error message

    def __init__(
        self,
        bag_file,
        topics_to_read,
        selected_topics_data,
        total_messages=None,
        frame_samples=1,
        topic_message_counts=None,
    ):
        super().__init__()
        self.bag_file = bag_file
        self.topics_to_read = topics_to_read
        self.selected_topics_data = selected_topics_data
        self.total_messages = total_messages
        self.frame_samples = frame_samples
        self.topic_message_counts = topic_message_counts

    def run(self):
        try:
            self.progress_updated.emit(20, "Reading messages from rosbag...")

            # Import here to avoid circular imports
            from .bag_handler import read_all_messages_optimized

            raw_messages = read_all_messages_optimized(
                self.bag_file,
                self.topics_to_read,
                self.progress_updated,
                self.total_messages,
                self.frame_samples,
                self.topic_message_counts,
            )

            self.progress_updated.emit(90, "Processing transformation data...")

            # Create topic types lookup
            topic_types = {}
            for topic_name, topic_type in self.topics_to_read.items():
                topic_types[topic_name] = topic_type

            self.progress_updated.emit(95, "Finalizing data...")

            # Signal completion
            self.processing_finished.emit(raw_messages, topic_types, self.selected_topics_data)

        except Exception as e:
            self.processing_failed.emit(str(e))
