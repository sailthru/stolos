
class QueueBackendBase(QueueBackendMixins):
    def get_zkclient():
    def get_qsize(app_name, queued=True, taken=True):
    def _queue(app_name, job_id, queue=True, priority=None):
    def check_if_queued(app_name, job_id):
    def maybe_add_subtask(app_name, job_id, timeout=5, queue=True, priority=None):
    def _obtain_lock(typ, app_name, job_id,
                     timeout=None, blocking=True, raise_on_error=False, safe=True):
    def is_execute_locked(app_name, job_id):
    def inc_retry_count(app_name, job_id, max_retry):
    def _recursively_reset_child_task_state(parent_app_name, job_id):
    def _set_state_unsafe(
        pending=False, completed=False, failed=False, skipped=False):
    def set_state(...)
    def check_state(
        app_name, job_id, raise_if_not_exists=False,
        pending=False, completed=False, failed=False, skipped=False,
        all=False, _get=False):
