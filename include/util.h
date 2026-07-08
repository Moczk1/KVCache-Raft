#pragma once
#include <iostream>
#include <utility>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

template <class F> class DeferClass
{
  public:
	explicit DeferClass(F &&f) : m_func(std::forward<F>(f)) {}
	explicit DeferClass(const F &f) : m_func(f) {}
	~DeferClass() { m_func(); }

	DeferClass(const DeferClass &e) = delete;
	DeferClass &operator=(const DeferClass &e) = delete;

  private:
	F m_func;
};

inline std::string GetTime()
{
	std::string time_s = "";
	time_t now = time(nullptr);
	tm *nowtm = localtime(&now);
	time_s = std::format("[{}-{}-{}-{}-{}-{}] ", nowtm->tm_year + 1900,
	    nowtm->tm_mon + 1, nowtm->tm_mday, nowtm->tm_hour, nowtm->tm_min,
	    nowtm->tm_sec);
	return time_s;
}

class Op
{
  public:
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	std::string Operation; // "Get" "Put" "Append"
	std::string Key;
	std::string Value;
	std::string ClientId; // 客户端号码
	int RequestId;        // 客户端号码请求的Request的序列号，为了保证线性一致性
	               //  IfDuplicate bool // Duplicate command can't be applied
	               //  twice , but only for PUT and APPEND

  public:
	// todo
	// 为了协调raftRPC中的command只设置成了string,这个的限制就是正常字符中不能包含|
	// 当然后期可以换成更高级的序列化方法，比如protobuf
	std::string asString() const
	{
		std::stringstream ss;
		boost::archive::text_oarchive oa(ss);

		// write class instance to archive
		oa << *this;
		// close archive

		return ss.str();
	}

	bool parseFromString(std::string str)
	{
		std::stringstream iss(str);
		boost::archive::text_iarchive ia(iss);
		// read class state from archive
		ia >> *this;
		return true; // todo : 解析失敗如何處理，要看一下boost庫了
	}

  public:
	friend std::ostream &operator<<(std::ostream &os, const Op &obj)
	{
		os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key +
		          "},Value{" + obj.Value + "},ClientId{" + obj.ClientId +
		          "},RequestId{" + std::to_string(obj.RequestId) +
		          "}"; // 在这里实现自定义的输出格式
		return os;
	}

  private:
	friend class boost::serialization::access;
	template <class Archive>
	void serialize(Archive &ar, const unsigned int version)
	{
		ar & Operation;
		ar & Key;
		ar & Value;
		ar & ClientId;
		ar & RequestId;
	}
};