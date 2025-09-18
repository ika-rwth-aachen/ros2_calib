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
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QThread, Signal
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

from . import ros_utils


# --- Helper functions (get_topic_info, etc.) remain the same ---
def get_topic_info(bag_file, ros_version="JAZZY") -> List[tuple]:
    """Get topic information from bag file."""
    topics = []
    ros_store = getattr(Stores, f"ROS2_{ros_version}")
    typestore = get_typestore(ros_store)
    with AnyReader([Path(bag_file).parent], default_typestore=typestore) as reader:
        for conn in reader.connections:
            topics.append((conn.topic, conn.msgtype, conn.msgcount))
    return topics

def get_total_message_count(bag_file, ros_version="JAZZY") -> int:
    """Get total message count from bag file."""
    ros_store = getattr(Stores, f"ROS2_{ros_version}")
    typestore = get_typestore(ros_store)
    with AnyReader([Path(bag_file).parent], default_typestore=typestore) as reader:
        return sum(conn.msgcount for conn in reader.connections)

def iterate_all_messages(bag_file: str, ros_version: str = "JAZZY"):
    """Generator to iterate through all messages in the rosbag efficiently."""
    ros_store = getattr(Stores, f"ROS2_{ros_version}")
    typestore = get_typestore(ros_store)
    with AnyReader([Path(bag_file).parent], default_typestore=typestore) as reader:
        for connection, timestamp, rawdata in reader.messages():
            msg = reader.deserialize(rawdata, connection.msgtype)
            yield timestamp, connection.topic, msg

def combine_tf_static_messages(tf_messages: List[Any]) -> Optional[Any]:
    """Combine multiple tf_static messages into one composite message."""
    if not tf_messages:
        return None
    if len(tf_messages) == 1:
        return tf_messages[0]

    base_message = tf_messages[0]
    all_transforms = list(getattr(base_message, 'transforms', []))
    seen = {(tf.header.frame_id, tf.child_frame_id) for tf in all_transforms}

    for msg in tf_messages[1:]:
        for tf in getattr(msg, 'transforms', []):
            key = (tf.header.frame_id, tf.child_frame_id)
            if key not in seen:
                all_transforms.append(tf)
                seen.add(key)

    if hasattr(base_message, 'transforms'):
        base_message.transforms = all_transforms
    return base_message

# --- CORRECTED ROBUST STREAMING SYNCHRONIZATION LOGIC ---
def read_synchronized_messages_streaming(
    bag_file: str,
    topics_to_read: Dict[str, str],
    progress_callback: Optional[Signal] = None,
    max_time_diff: float = 0.1,
    ros_version: str = "JAZZY",
) -> Dict:
    """
    Finds the best synchronized pair of messages using a memory-efficient streaming
    (two-pointer) algorithm, robustly handling leading slashes in topic names.
    """
    sensor_topics = {t: mt for t, mt in topics_to_read.items() if "PointCloud2" in mt}
    if len(sensor_topics) != 2:
        raise ValueError("Synchronization requires exactly 2 PointCloud2 topics.")

    # Normalize the target topic names by removing any leading slash
    topic1 = list(sensor_topics.keys())[0]
    topic2 = list(sensor_topics.keys())[1]
    topic1_norm = topic1.lstrip('/')
    topic2_norm = topic2.lstrip('/')

    if progress_callback:
        progress_callback.emit(10, f"Synchronizing {topic1} and {topic2}...")

    best_pair = None
    min_diff = float('inf')

    ros_store = getattr(Stores, f"ROS2_{ros_version}")
    typestore = get_typestore(ros_store)

    with AnyReader([Path(bag_file).parent], default_typestore=typestore) as reader:
        # *** FIX: Find connections by comparing normalized topic names ***
        conn1, conn2 = None, None
        for c in reader.connections:
            normalized_conn_topic = c.topic.lstrip('/')
            if normalized_conn_topic == topic1_norm:
                conn1 = c
            elif normalized_conn_topic == topic2_norm:
                conn2 = c

        # *** FIX: Provide a much more informative error message ***
        if not conn1 or not conn2:
            available_pc_topics = [
                c.topic for c in reader.connections if "PointCloud2" in c.msgtype
            ]
            error_msg = (
                f"One or both topics not found in the bag file.\n"
                f"Looking for: ['{topic1}', '{topic2}']\n"
                f"Available PointCloud2 topics in bag: {available_pc_topics}"
            )
            raise ValueError(error_msg)

        iter1 = reader.messages(connections=[conn1])
        iter2 = reader.messages(connections=[conn2])

        try:
            _, t1, raw_msg1 = next(iter1)
            _, t2, raw_msg2 = next(iter2)
        except StopIteration:
            raise ValueError("One or both topics have no messages.")

        max_diff_ns = int(max_time_diff * 1e9)

        while True:
            diff = abs(t1 - t2)
            if diff < max_diff_ns:
                if diff < min_diff:
                    min_diff = diff
                    best_pair = {
                        topic1: reader.deserialize(raw_msg1, conn1.msgtype),
                        topic2: reader.deserialize(raw_msg2, conn2.msgtype)
                    }
                try:
                    _, t1, raw_msg1 = next(iter1)
                    _, t2, raw_msg2 = next(iter2)
                except StopIteration:
                    break
            elif t1 < t2:
                try:
                    _, t1, raw_msg1 = next(iter1)
                except StopIteration:
                    break
            else:
                try:
                    _, t2, raw_msg2 = next(iter2)
                except StopIteration:
                    break

    if best_pair is None:
        raise ValueError(f"No synchronized pairs found within {max_time_diff}s time window")

    if progress_callback:
        progress_callback.emit(85, f"Best pair found with {min_diff / 1e6:.2f}ms difference.")

    tf_messages = []
    tf_topics = {t for t in topics_to_read if "tf_static" in t}
    if tf_topics:
        with AnyReader([Path(bag_file).parent], default_typestore=typestore) as reader:
            tf_connections = [c for c in reader.connections if c.topic in tf_topics]
            if tf_connections:
                for _, _, rawdata in reader.messages(connections=tf_connections):
                    tf_messages.append(reader.deserialize(rawdata, tf_connections[0].msgtype))

    final_messages = best_pair
    if tf_messages:
        final_messages[list(tf_topics)[0]] = combine_tf_static_messages(tf_messages)

    return final_messages


def read_all_messages_optimized(
    bag_file: str,
    topics_to_read: dict,
    progress_callback=None,
    total_messages=None,
    frame_samples: int = 6,
    topic_message_counts=None,
    ros_version: str = "JAZZY",
) -> dict:
    """Read multiple uniformly sampled frame messages from rosbag."""
    sensor_topics = {t: mt for t, mt in topics_to_read.items() if "tf_static" not in t}
    topic_counts = {t: topic_message_counts.get(t, 0) for t in sensor_topics.keys()}
    sampling_intervals = {t: max(1, c // frame_samples) for t, c in topic_counts.items() if c > 0}

    messages = {}
    tf_static_messages = []
    msg_counters = {topic: 0 for topic in sensor_topics.keys()}
    collected_samples = {topic: [] for topic in sensor_topics.keys()}

    processed_count = 0
    for timestamp, topic, msg_data in iterate_all_messages(bag_file, ros_version):
        processed_count += 1
        if progress_callback and total_messages and processed_count % 100 == 0:
            progress = int((processed_count / total_messages) * 70) + 20
            progress_callback.emit(min(progress, 90), f"Sampling frames {processed_count}/{total_messages}...")

        if topic in topics_to_read:
            if "tf_static" in topic:
                tf_static_messages.append(msg_data)
            elif topic in sensor_topics:
                msg_counters[topic] += 1
                interval = sampling_intervals.get(topic, 1)
                if (msg_counters[topic] - 1) % interval == 0 and len(collected_samples[topic]) < frame_samples:
                    collected_samples[topic].append({"timestamp": timestamp, "data": msg_data, "topic_type": topics_to_read[topic]})

    messages["frame_samples"] = collected_samples
    if tf_static_messages:
        tf_topic = next((t for t in topics_to_read if "tf_static" in t), None)
        if tf_topic:
            messages[tf_topic] = combine_tf_static_messages(tf_static_messages)

    if progress_callback:
        progress_callback.emit(90, f"Collected {sum(len(s) for s in collected_samples.values())} frame samples.")
    return messages


def convert_to_mock(raw_msg, msg_type):
    if msg_type == "sensor_msgs/msg/Image":
        return ros_utils.Image(
            header=raw_msg.header,
            height=raw_msg.height,
            width=raw_msg.width,
            encoding=raw_msg.encoding,
            is_bigendian=raw_msg.is_bigendian,
            step=raw_msg.step,
            data=raw_msg.data)
    if msg_type == "sensor_msgs/msg/CompressedImage":
        # Create a mock CompressedImage with the necessary attributes
        mock_img = ros_utils.Image(
            header=raw_msg.header,
            encoding=raw_msg.format,
            data=raw_msg.data,
            height=0,  # CompressedImages don't have height/width in the message
            width=0,   # These will be determined when the image is decoded
            is_bigendian=False,
            step=0
        )
        mock_img._type = "sensor_msgs/msg/CompressedImage"
        return mock_img
    elif msg_type == "sensor_msgs/msg/PointCloud2":
        fields = [ros_utils.PointField(name=f.name, offset=f.offset, datatype=f.datatype, count=f.count) for f in raw_msg.fields]
        return ros_utils.PointCloud2(
            header=raw_msg.header,
            height=raw_msg.height,
            width=raw_msg.width,
            fields=fields,
            is_bigendian=raw_msg.is_bigendian,
            point_step=raw_msg.point_step,
            row_step=raw_msg.row_step,
            data=raw_msg.data,
            is_dense=raw_msg.is_dense)
    elif msg_type == "sensor_msgs/msg/CameraInfo":
        return ros_utils.CameraInfo(
            header=raw_msg.header,
            height=raw_msg.height,
            width=raw_msg.width,
            distortion_model=raw_msg.distortion_model,
            d=raw_msg.d,
            k=raw_msg.k,
            r=raw_msg.r,
            p=raw_msg.p)
    return raw_msg


class RosbagProcessingWorker(QThread):
    """Worker thread for processing rosbag data without blocking the UI."""

    progress_updated = Signal(int, str)
    processing_finished = Signal(dict, dict, dict)
    processing_failed = Signal(str)

    def __init__(
        self,
        bag_file: str,
        topics_to_read: Dict,
        selected_topics_data: Dict,
        total_messages: Optional[int] = None,
        frame_samples: int = 1,
        topic_message_counts: Optional[Dict] = None,
        ros_version: str = "JAZZY",
        sync_tolerance: float = 0.05,
    ):
        super().__init__()
        self.bag_file = bag_file
        self.topics_to_read = topics_to_read
        self.selected_topics_data = selected_topics_data
        self.total_messages = total_messages
        self.frame_samples = frame_samples
        self.topic_message_counts = topic_message_counts
        self.ros_version = ros_version
        self.sync_tolerance = sync_tolerance

    def run(self):
        try:
            self.progress_updated.emit(5, "Preparing to read rosbag...")
            calibration_type = self.selected_topics_data.get("calibration_type", "LiDAR2Cam")

            if calibration_type == "LiDAR2LiDAR":
                raw_messages = read_synchronized_messages_streaming(
                    self.bag_file,
                    self.topics_to_read,
                    self.progress_updated,
                    max_time_diff=self.sync_tolerance,
                    ros_version=self.ros_version,
                )
            else:
                raw_messages = read_all_messages_optimized(
                    self.bag_file,
                    self.topics_to_read,
                    self.progress_updated,
                    self.total_messages,
                    self.frame_samples,
                    self.topic_message_counts,
                    self.ros_version,
                )

            self.progress_updated.emit(95, "Finalizing data...")
            topic_types = {name: type_str for name, type_str in self.topics_to_read.items()}
            self.processing_finished.emit(raw_messages, topic_types, self.selected_topics_data)

        except Exception as e:
            import traceback
            print(f"ERROR in RosbagProcessingWorker: {traceback.format_exc()}")
            self.processing_failed.emit(f"Processing failed: {e}")
