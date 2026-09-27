"""dp1 湖底座 LakeStore（票 06 实装；本文件现承载快照语义异常类型）。"""


class SnapshotGoneError(Exception):
    """快照不存在或已过保留窗口——绝不静默回退当前快照（DP-W-D7-3）。"""
