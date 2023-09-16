#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <variant>

#include "Iouring.hpp"
#include "Util.hpp"

/*
	+---------------------------------+
	|            Page Header =        |
	|		  number of records		  |
	+---------------------------------+
	|           Record 0              |
	+---------------------------------+
	|           Record 1.             |
	+---------------------------------+
	|             ....                |
	+---------------------------------+
	|           Record N              |
	+---------------------------------+ 
	|                                 |
	|           FREE SPACE            |
	|              ....               | 
	+---------------------------------+
*/

static constexpr int32_t HEADER_SIZE = sizeof(int32_t); 

enum class PageResponse {
	PageFull,
	PageEmpty,
	InvalidOffset,
	InvalidRecord,
	DeletedRecord,
	Success
};

struct RecordResponse {
	Record       record;
	PageResponse status;
};

struct PageHandler {
	void init_handler(Page*			     page,
					  const RecordLayout layout,
					  const int32_t	     p_id,
					  const int32_t	     fd,
					  const PageType     p_type);
	
	PageResponse write_to_page (off_t&	    write_offset, 
							    RecordData& record_data, 
							    const DatabaseType& db_type);
	off_t		 read_from_page(const off_t read_offset, 
							    RecordData& record_data, 
							    const DatabaseType& db_type);

	PageResponse   add_record	(Record& record);
	PageResponse   delete_record(const uint32_t record_num);
	RecordResponse read_record	(const uint32_t record_num);
	PageResponse   move_record  (const uint32_t new_record_num, 
								 Record& record);
	PageResponse   compact_page	();
	
	void update_num_records() {
		std::memcpy(page_ptr, &num_records, 
					sizeof(num_records));
	}
	
	void prep_for_disk() {
		compact_page();
		update_num_records();
	}

	int32_t get_num_records() {
		return *reinterpret_cast<int32_t*>(page_ptr);
	}

	bool in_tomb_stones(const uint32_t record_num) {
		return std::find(std::begin(tomb_stones), std::end(tomb_stones), record_num) != 
			   std::end(tomb_stones);
	}

	off_t record_num_to_offset(const uint32_t record_num) {
		return HEADER_SIZE + record_num * record_size;
	}

	bool is_pinned = false;
	bool is_dirty  = false;
	
	int32_t	 num_records = 0; 
	int32_t  page_fd     = -1;
	int32_t	 page_usage  = 0;
	int32_t  page_id	 = 0;
	off_t	 page_cursor = 0;
	size_t	 record_size = 0;	
	Page*	 page_ptr    = nullptr;
	PageType page_type;
	
	RecordLayout          record_layout;
	std::vector<uint32_t> tomb_stones;
};
