#ifndef _THREAD_POOL_H_
#define _THREAD_POOL_H_

#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <vector>
#include <queue>

class ThreadPool
{
public:
	using Task = std::function<void()>;

	explicit ThreadPool(int num): _thread_num(num), _is_running(false)
	{}

	~ThreadPool()
	{
		if (_is_running)
			stop();
	}

	void start()
	{
		_is_running = true;
        _is_working = new std::atomic_bool[_thread_num];
        token = 0;
		// start threads
		for (int i = 0; i < _thread_num; i++)
			_threads.emplace_back(std::thread(&ThreadPool::work, this ,i));
	}

	void stop()
	{
		{
			// stop thread pool, should notify all threads to wake
			std::unique_lock<std::mutex> lk(_mtx);
			_is_running = false;
			for(int i=0;i<_thread_num;i++){
                _is_working[i]=true;   //避免线程死循环
            }
		}

		// terminate every thread job
		for (std::thread& t : _threads)
		{
			if (t.joinable())
				t.join();
		}
	}

	void appendTask(const Task& task)
	{
		if (_is_running)
		{
			std::unique_lock<std::mutex> lk(_mtx);   //自动解锁
			_tasks.push(task);
            int i=0;
			for(;i<_thread_num;i++){
                if(_is_working[(i+token)%_thread_num]==false){
                    _is_working[(i+token)%_thread_num]=true;
                    break;
                }
            }
            token=(i+token)%_thread_num;
		}
	}

private:
	void work(int num)
	{
		// printf("begin work thread: %d\n", std::this_thread::get_id());

		// every thread will compete to pick up task from the queue to do the task
		while (_is_running)
		{
			Task task;
			{
				_mtx.lock();
				if (!_tasks.empty())
				{
					// if tasks not empty, 
					// must finish the task whether thread pool is running or not
					task = _tasks.front();
					_tasks.pop(); // remove the task
                    _mtx.unlock();
				}
				else if (_is_running && _tasks.empty())
                {
                    _mtx.unlock();
                    _is_working[num]=false;
                    while(!_is_working[num]){
                        // std::this_thread::yield();
                        asm("nop");
                    }
                }
			}

			if (task)
				task(); // do the task
		}

		// printf("end work thread: %d\n", std::this_thread::get_id());
	}

public:
	// disable copy and assign construct
	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool& other) = delete;

private:
	std::atomic_bool _is_running; // thread pool manager status
	std::mutex _mtx;
	int _thread_num;
    int token;
	std::vector<std::thread> _threads;
	std::queue<Task> _tasks;
    std::atomic_bool *_is_working;
};


#endif // !_THREAD_POOL_H_