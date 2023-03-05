#include <algorithm>
#include <condition_variable>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <thread>
#include <vector>

enum class JobState { Notstarted, Ongoing, Finished };

struct IJob {
  virtual const JobState &GetState() const = 0;
  virtual void run() = 0;

  virtual ~IJob() {}
};

struct NoOwingJob : IJob {
  IJob &mJob;
  NoOwingJob(IJob &job) : mJob(job) {}
  const JobState &GetState() const { return mJob.GetState(); };
  void run() { mJob.run(); };
};

struct Job : public IJob {
  JobState mState;

  void run() { mState = JobState::Ongoing; };
  const JobState &GetState() const override { return mState; }
};

struct BatchJobs {
  struct JobOfBatchJob : IJob {
    std::unique_ptr<IJob> mJob;
    BatchJobs &mBatchJobParent;
    JobOfBatchJob(BatchJobs &batchJobParent, std::unique_ptr<IJob> job)
        : mBatchJobParent(batchJobParent), mJob(std::move(job)) {}
    void run() {
      mJob->run();
      std::lock_guard<std::mutex> lk(mBatchJobParent.cv_m);
      mBatchJobParent.mJobsUnfinished--;
      mBatchJobParent.cv.notify_all();
    }

    const JobState &GetState() const override { return mJob->GetState(); }
  };

  size_t mJobsUnfinished;
  std::condition_variable cv;
  std::mutex cv_m;

  BatchJobs(int amount = 0) : mJobsUnfinished(amount) {}

  BatchJobs(std::vector<std::unique_ptr<IJob>> &jobs)
      : mJobsUnfinished(jobs.size()) {
    std::for_each(jobs.begin(), jobs.end(), [&](auto &job) { ModifyJob(job); });
  }

  void ModifyJob(std::unique_ptr<IJob> &job) {
    auto temp_job = std::move(job);
    job = std::make_unique<JobOfBatchJob>(*this, std::move(temp_job));
  }

  void WaitTillEmpty() {
    std::unique_lock<std::mutex> lk(cv_m);
    cv.wait(lk, [&] { return mJobsUnfinished == 0; });
    int w = 0;
  }
};

struct JobQueue {
  std::counting_semaphore<100> mNumberOfJobs;
  std::mutex mLock;
  std::list<std::unique_ptr<IJob>> mJobs;

  JobQueue() : mNumberOfJobs(0) {}

  void enqueue(std::unique_ptr<IJob> job) {
    mLock.lock();
    mJobs.emplace_back(std::move(job));
    mLock.unlock();
    mNumberOfJobs.release(1);
  }

  template <typename It> void enqueue_range(It begin, It end) {
    mLock.lock();
    size_t jobs_added = 0;
    for (auto it = begin; it != end; it++) {
      mJobs.emplace_back(std::move(*it));
      jobs_added++;
    }
    mLock.unlock();
    mNumberOfJobs.release(jobs_added);
  }

  template <typename Func> void enqueue_func(int num, Func &&func) {
    mLock.lock();
    size_t jobs_added = 0;
    for (int i = 0; i < num; ++i) {
      mJobs.emplace_back(func(i));
      jobs_added++;
    }
    mLock.unlock();
    mNumberOfJobs.release(jobs_added);
  }

  std::unique_ptr<IJob> request_job() {
    mNumberOfJobs.acquire();
    mLock.lock();
    mJobs.front();
    auto job = std::move(mJobs.front());
    mJobs.pop_front();
    mLock.unlock();
    return job;
  }
};

struct Runner {
  std::thread mThread;
  bool mKeepRunning;
  JobQueue &mJobQueue;

  Runner(JobQueue &jobQueue) : mJobQueue(jobQueue) {}

  void run() {
    mThread = std::thread([this]() {
      while (this->mKeepRunning) {
        auto job = mJobQueue.request_job();
        job->run();
      }
    });
  }
};

struct ThreadPool {
  std::vector<Runner> runners;
  ThreadPool(std::vector<Runner> runners) : runners(std::move(runners)) {}
  void run() {
    for (auto &runner : runners) {
      runner.run();
    }
  }
};

ThreadPool ConstructThreadPool(JobQueue &jobQueue, int num) {
  std::vector<Runner> runners;
  runners.reserve(num);
  for (int i = 0; i < num; ++i) {
    runners.emplace_back(jobQueue);
  }

  return {std::move(runners)};
}

struct Scheduler;

struct SchedulerHandler {
  virtual void OnQueueEmpty(Scheduler &) = 0;
  virtual void OnBatchFinished() = 0;
  virtual bool KeepRunning() = 0;
  virtual ~SchedulerHandler() {}
};

struct Scheduler {
  ThreadPool &mThreadPool;
  JobQueue &mJobQueue;
  std::optional<BatchJobs> mCurrentBatchJobs;
  SchedulerHandler &handler;

  Scheduler(ThreadPool &threadPool, JobQueue &jobQueue,
            SchedulerHandler &handler)
      : mThreadPool(threadPool), mJobQueue(jobQueue), handler(handler) {}

  void run() {
    mThreadPool.run();
    while (handler.KeepRunning()) {
      handler.OnQueueEmpty(*this);
      mCurrentBatchJobs->WaitTillEmpty();
      handler.OnBatchFinished();
    }
  }

  void ScheduleBatch(std::vector<std::unique_ptr<IJob>> &jobs) {
    mCurrentBatchJobs.emplace(jobs);
    mJobQueue.enqueue_range(jobs.begin(), jobs.end());
  }

  template <typename Func> void ScheduleBatchFunc(int num, Func &&func) {
    mCurrentBatchJobs.emplace(num);
    mJobQueue.enqueue_func(num, [&](auto i) {
      std::unique_ptr<IJob> job = func(i);
      mCurrentBatchJobs->ModifyJob(job);
      return job;
    });
  }
};

struct MyJob : Job {
  std::vector<int> mNums;
  int sum = 0;
  void run() override {
    sum = 0;
    for (int i = 0; i < mNums.size(); ++i) {
      sum += mNums[i];
    }
    int w = 0;
  }
};

struct MySchedulerHandler : public SchedulerHandler {
  int mNumber_executions;
  int mNumber_jobs;
  std::vector<MyJob> jobs;

  MySchedulerHandler(int number_executions, int number_jobs)
      : mNumber_executions(number_executions), mNumber_jobs(number_jobs) {
    for (int i = 0; i < mNumber_jobs; ++i) {
      std::vector<int> ints;
      jobs.emplace_back();
    }
  }

  void OnQueueEmpty(Scheduler &scheduler) override {
    // read in some "data"
    for (int i = 0; i < mNumber_jobs; ++i) {
      auto size = rand() % 1000;
      jobs[i].mNums.clear();
      jobs[i].mNums.resize(size);
      for (int j = 0; j < size; ++j) {
        jobs[i].mNums[j] = rand() % 100;
      }
    }
    scheduler.ScheduleBatchFunc(jobs.size(), [&](auto i) {
      return std::make_unique<NoOwingJob>(jobs[i]);
    });
  };

  void OnBatchFinished() {
    std::vector<int> results(jobs.size(), 0);
    for (int i = 0; i < jobs.size(); ++i) {
      results[i] = jobs[i].sum;
    }
    std::sort(results.begin(), results.end());

    for (auto &result : results) {
      std::cout << result << ",";
    }

    std::cout << std::endl;
  }

  bool KeepRunning() override { return mNumber_executions != 0; };
};

int main() {
  int w = 0;
  MySchedulerHandler schedulerHandler(10, 2);
  JobQueue jobQueue;
  ThreadPool pool = ConstructThreadPool(jobQueue, 4);
  Scheduler scheduler(pool, jobQueue, schedulerHandler);

  scheduler.run();

  return 0;
}
