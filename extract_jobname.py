import re

import re

def extract_job_name(subscription_path):
    match = re.search(r'subscriptions/([^/]+?)--', subscription_path)
    if match:
        job_name = match.group(1)
        if "Job" in job_name:
            # Return up to and including "Job"
            job_index = job_name.find("Job") + len("Job")
            return job_name[:job_index]
        else:
            return job_name


    return re.search(r'subscriptions/([^/]+?)(?:-sub)?$', subscription_path).group(1)

print(extract_job_name('projects/fkp-dart-pubsub/subscriptions/in-chennai-1.dart.FKMP.sp.product.listing-sub'))
