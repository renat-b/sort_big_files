//      222222222222222222
//      strfile-parser.cpp
//      
//      Copyright 2011 Trusov Denis
//      
#define _FILE_OFFSET_BITS 64

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <algorithm>

#ifndef WINDOWS
   #include <stdio.h>
   #include <sys/types.h>
   #include <sys/stat.h>
   #include <fcntl.h>   
   #include <unistd.h>
#else
    #include "windows.h"
#endif


int file_open_read(const char *name)
{
#ifdef WINDOWS
    HANDLE hf;
    
    hf=CreateFile(name,GENERIC_READ,FILE_SHARE_READ,NULL,OPEN_EXISTING,FILE_ATTRIBUTE_NORMAL,NULL);
    if(hf==INVALID_HANDLE_VALUE)
    {
        return(-1);
    }
    return((int)hf);
#else
    fd = open (file_name.c_str(), O_RDONLY);
#endif
}

int file_open_write(const char *name)
{
#ifdef WINDOWS
    HANDLE hf;
    
    hf=CreateFile(name,GENERIC_READ|GENERIC_WRITE,0,NULL,OPEN_ALWAYS,FILE_ATTRIBUTE_NORMAL,NULL);
    if(hf==INVALID_HANDLE_VALUE)
        return(-1);

    return((int)hf);
#else
    fd = open (filename.c_str(), O_WRONLY|O_TRUNC|O_CREAT|O_APPEND, S_IRWXU);
#endif
}

void file_close(int fd)
{
#ifdef WINDOWS
    CloseHandle((HANDLE)fd);
#else
    close(fd);
#endif
}

int64_t file_read(int fd,char *buffer,int length)
{
#ifdef WINDOWS
    DWORD   readed;

    if(ReadFile((HANDLE)fd,buffer,length,&readed,NULL)==FALSE)
        return(-1);
    return(readed);
#else
    return (read (fd, buffer, length));
#endif
}

int64_t file_write(int fd,const char *buffer,int length)
{
#ifdef WINDOWS
    DWORD writed;

    if(WriteFile((HANDLE)fd,buffer,length,&writed,NULL)==FALSE)
        return(-1);
    return(writed);
#else
    return (write (fd, buffer, length));
#endif
}

int64_t file_seek(int fd,int64_t offset)
{
#ifdef WINDOWS
    LONG low,hight;

    low  =((LONG)(((int64_t)(offset)) & 0xffffffff));
    hight=((LONG)((((int64_t)(offset)) >> 32) & 0xffffffff));  
    SetFilePointer((HANDLE)fd,low,&hight,FILE_BEGIN);
    return(offset);
#else
    return (lseek64 (fd, offset, SEEK_SET));
#endif
}

double calctime ()
{
#ifndef WINDOWS
	static double ot = 0.0;
	timespec stamp;
	clock_gettime (CLOCK_REALTIME, &stamp);
	double ct = (stamp.tv_sec + (1.0e-9*stamp.tv_nsec));
	double pt = ot;
	ot = ct;
	return (ct - pt);
#else
    return(0.0);
#endif    
}

using namespace std;

//buffer should be enough size to contain string of maximum length from source file
#define MY_BUFFER_SIZE (32*1024*1024)
#define MY_PAGE_SIZE (16*1024)

typedef
	unsigned char byte;

typedef
	vector <unsigned>      str_entries;

class file_parser
{
	public:
		file_parser (string file_name, unsigned buffer_size = MY_BUFFER_SIZE);
		~file_parser ();
		void next (str_entries &table);
		bool empty ();
		string line_from_buffer (unsigned offset);
		const byte *strptr_from_buffer (unsigned offset);

	private:
		void read ();
		// members
		vector <byte> buffer;
		int      fd;
		bool     no_more_bytes;
		unsigned remain_count;
		unsigned current_offset;
		unsigned progress_count;
		
};

const byte *file_parser::strptr_from_buffer (unsigned offset)
{
	unsigned size = buffer.size();
	if ((offset < current_offset) || (offset > current_offset + size))
	{
		clog << "offset " << offset << endl;
		clog << "current offset " << current_offset << endl;
		clog << "current_offset + size " << current_offset + size << endl;
		throw;
	}
 return &buffer[offset-current_offset];
}

string file_parser::line_from_buffer (unsigned offset)
{
	unsigned size = buffer.size();
	string result;
	if ((offset < current_offset) || (offset > current_offset + size))
	{
		clog << "offset "                << offset << endl;
		clog << "current offset "        << current_offset << endl;
		clog << "current_offset + size " << current_offset + size << endl;
		throw;
	}
	else
	{
		unsigned l = 0;
		for (unsigned i = offset-current_offset; i < size; i++)
			if (buffer[i] != '\n')
				//result += buffer[i];
				l++;
			else
				break;
		result = string((char *)&buffer[offset-current_offset], l);
	}
 return result;
}

void file_parser::next (str_entries & table)
{
	calctime();
	clog << string(21, '-') << endl;
	this->read();

	for (unsigned i = remain_count; i < buffer.size(); i++)
	{
	    progress_count += 1;
	    remain_count   += 1;
	    //cout << buffer[i];
	    if (buffer[i] == '\n')
	    {
	        table.push_back (progress_count-remain_count);
	        //cout << "*"+string ((char *)&buffer[i+1 - remain_count], remain_count-1).substr(0,50) << endl;
	        //cout << "+"+line_from_buffer(progress_count-remain_count).substr(0,50) << endl;
	        cout << flush;
	        //clog << string ((char *)&buffer[i+1 - remain_count], remain_count);
	        //clog << flush;
	        remain_count = 0;
	    }
	}


	//cout << flush;
	clog << endl;
	clog << "operation time " << calctime()       << endl;
	clog << "entries count "  << table.size()     << endl;
	clog << "capacity count " << table.capacity() << endl;
	clog << "progress "       << progress_count   << endl;
	clog << "remainder "      << remain_count     << endl;
	clog << "buffer size "    << buffer.size()    << endl;
	clog << flush;
}

void file_parser::read ()
{
	current_offset = progress_count - remain_count;
	// move to front remain bytes
	for (unsigned i = 0; i < remain_count; i++)
		buffer[i] = buffer[buffer.size() - remain_count + i];

	unsigned count;
	//count = ::read (fd, &buffer[remain_count], buffer.size()-remain_count);
    count = (int)file_read (fd, (char*)&buffer[remain_count], buffer.size()-remain_count);
	if (count < buffer.size()-remain_count)
	{
	    if (remain_count + count > 0)
	    {
	        if (buffer[remain_count+count-1] != '\n')
	        	count++;
	        buffer[remain_count+count-1] = '\n';
	    }
	    buffer.resize (remain_count+count);
	    no_more_bytes = true;
	}
	else
		if (count == unsigned(-1))
		{
		    clog << "read file error" << endl;
		    throw;
		}
            else if (count == 0)
            {
                cerr << "string length too long" << endl;
                throw;
            }
	clog << "read " << count << endl;
}

bool file_parser::empty ()
{
	return no_more_bytes;
}

file_parser::file_parser (string file_name, unsigned buffer_size):
	buffer (buffer_size)
{
	//fd = open (file_name.c_str(), O_RDONLY);
    fd = file_open_read(file_name.c_str());
	if (fd == -1) 
	{
		clog << "open file error" << endl;
		throw;
	}
	remain_count   = 0;
	progress_count = 0;	
	no_more_bytes  = false;
	clog << "buffer size " << buffer.size() << endl;
}

file_parser::~file_parser ()
{
    file_close(fd);
}

class cmp_functor
{
	public:
		cmp_functor (file_parser &file) : file(file) {}
		bool operator () (unsigned i, unsigned j)
		{
			const byte * str2 = file.strptr_from_buffer(j);
			const byte * str1 = file.strptr_from_buffer(i);
			while ((*str1 != '\n')&&(*str2 != '\n'))
			{
				if (*str1 == *str2)
					str1++, str2++;
				else
					return (*str2 < *str1);
			}
                        if (*str1 != '\n')
                            return true;
			//if (*str2 != '\n') return false;
			return false;
			//return (file.line_from_buffer(j) < file.line_from_buffer(i));
		}
	private:
                file_parser &file;
};

class temp_file
{
	public:
		temp_file (const string & filename);
		~temp_file ();
		unsigned writelines (str_entries & table, file_parser & buffer);
	private:
		int fd;
};

unsigned temp_file::writelines (str_entries & table, file_parser & buffer)
{
	string   line;
	unsigned count = 0;

	while (!table.empty())
	{
		line   = buffer.line_from_buffer(table.back())+'\n';
		count += line.length();
		table.pop_back();
		//write (fd, line.c_str(), line.length());
        file_write (fd, line.c_str(), line.length());
	}
	//write (fd, string("------------\n").c_str(), string("------------\n").length());
 return count;
}

temp_file::temp_file (const string & filename)
{
	//fd = open (filename.c_str(), O_WRONLY|O_TRUNC|O_CREAT|O_APPEND, S_IRWXU);
    fd = file_open_write (filename.c_str());
	if (fd == -1)
	{
		cerr << "temp file open fail" << endl;
		throw;
	}
}

temp_file::~temp_file ()
{
	file_close (fd);
}

typedef
	byte page[MY_PAGE_SIZE];

struct chunk
{
	unsigned count;
	unsigned offset;
	unsigned size;
	unsigned current_offset;
	page bufptr;	
};

class chunks_merger
{
	public:
		chunks_merger (string & filename, vector <unsigned> &table);
		~chunks_merger ();
	private:
		string next_line_from_chunk (unsigned chunk_id);
		byte byte_from_chunk (chunk &buffer, unsigned index);
		bool chunk_empty (chunk &buffer);
		vector <chunk> buffers;
		int fd;
};

bool chunks_merger::chunk_empty (chunk &buffer)
{
	return !(buffer.count < buffer.size);
}
string chunks_merger::next_line_from_chunk (unsigned chunk_id)
{
	string result;
	chunk & buffer = buffers[chunk_id];
	if (buffer.count < buffer.size)
	{
		while ((buffer.count < buffer.size) && (result[result.size()-1] != '\n'))
		{
			result += byte_from_chunk(buffer, buffer.count);
			buffer.count += 1;
		}
	}
	else
	{
		cerr << "line from chunk error" << endl;
		throw;
	}
 return result;
}

byte chunks_merger::byte_from_chunk (chunk &buffer, unsigned index)
{
 byte result;
	if (index < buffer.size)
	{
		if ((buffer.offset + index < buffer.current_offset)||(buffer.offset + index >= buffer.current_offset + sizeof(page)))
		{			
			//int e = lseek64 (fd, buffer.offset + index, SEEK_SET);
            int e = (int)file_seek(fd,buffer.offset + index);
			//int c = read (fd, buffer.bufptr, sizeof(page));
            int c = (int)file_read (fd, (char*)buffer.bufptr, sizeof(page));
			e++, c++;
			buffer.current_offset = buffer.offset + index;
			//clog << "readpage " << buffer.offset + index << endl;
		}

		//clog << buffer.offset << " " << index << " " << buffer.current_offset << endl;
		//clog << buffer.offset + index << " " << buffer.offset + index - buffer.current_offset << endl;
		//clog << sizeof(page) << endl;
		
		result = buffer.bufptr[buffer.offset + index - buffer.current_offset];
	}
	else
	{
		cerr << "byte from chunk error" << endl;
		throw;
	}
 return result;
}

chunks_merger::~chunks_merger ()
{
	file_close(fd);
}

chunks_merger::chunks_merger (string & filename, vector <unsigned> &table)
{
	unsigned offset = 0;
	for (unsigned i = 0; i < table.size(); i++)
	{
		buffers.push_back(chunk());
		//cout << table[i] << endl;
		buffers[i].count = 0;
		buffers[i].offset = offset;
		buffers[i].size = table[i];
		buffers[i].current_offset = offset + table[i];
		offset += table[i];
		//buffers.push_back(buffer);
	}
	//fd = open (filename.c_str(), O_RDONLY);
    fd = file_open_read (filename.c_str());
	vector <string> str_cache;
	str_cache.resize(buffers.size());
	for (unsigned i = 0; i < buffers.size(); i++)
		str_cache[i] = next_line_from_chunk(i);
	unsigned m, i;
	while (!buffers.empty())
	{
		for (i = 1, m = 0; i < str_cache.size(); i++)
		{
			if (str_cache[i] < str_cache[m])
				m = i;
		}
		cout << str_cache[m];
		if (!chunk_empty(buffers[m]))
			str_cache[m] = next_line_from_chunk(m);
		else
		{
			buffers.erase (buffers.begin()+m);
			str_cache.erase (str_cache.begin()+m);
		}
	}
}

int main(int argc, char **argv)
{
    string srcname, tmpname;
    
    clog << "Command line example:" << endl;
#ifndef WINDOWS
    srcname =  "/home/user/tmp/big_file.txt";
    tmpname =  "/home/user/tmp/big_file_tmp.txt";
#else
    srcname =  "c:/tmp/big_file.txt";
    tmpname =  "c:/tmp/big_file_tmp.txt";
#endif
    vector <unsigned> table_chunks_sizes;
    
    file_parser file_source (srcname);
    temp_file   file_temp   (tmpname);
    str_entries table;
    while (!file_source.empty())
    {
        file_source.next (table);
        if (!table.empty())
        {
            calctime();
            sort (table.begin(), table.end(), cmp_functor(file_source));
            clog << "sort operation time " << calctime() << endl;
            table_chunks_sizes.push_back(file_temp.writelines(table, file_source));
            clog << "write operation time " << calctime() << endl;
        }
    }
    
    calctime();
    chunks_merger (tmpname, table_chunks_sizes);
    clog << "merge time " << calctime() << endl;
    return 0;
}
