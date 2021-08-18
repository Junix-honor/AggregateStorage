////
// @file block.cc
// @brief
// 实现block
//
//

#include <db/block.h>
#include <db/record.h>
#include <db/block.h>

namespace db {

void Block::clear(int spaceid, int blockid)
{
    spaceid = htobe32(spaceid);
    blockid = htobe32(blockid);
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + BLOCK_MAGIC_OFFSET, &BLOCK_MAGIC_NUMBER, BLOCK_MAGIC_SIZE);
    // 设置spaceid
    ::memcpy(buffer_ + BLOCK_SPACEID_OFFSET, &spaceid, BLOCK_SPACEID_SIZE);
    // 设置blockid
    ::memcpy(buffer_ + BLOCK_NUMBER_OFFSET, &blockid, BLOCK_NUMBER_SIZE);
    // 设置usedspace
    setUsedspace(0);
    // 设置freespace
    unsigned short data = htobe16(BLOCK_DEFAULT_FREESPACE);
    ::memcpy(buffer_ + BLOCK_FREESPACE_OFFSET, &data, BLOCK_FREESPACE_SIZE);
    // 设置checksum
    int checksum = BLOCK_DEFAULT_CHECKSUM;
    ::memcpy(buffer_ + BLOCK_CHECKSUM_OFFSET, &checksum, BLOCK_CHECKSUM_SIZE);
}

void Root::clear(unsigned short type)
{
    // 清buffer
    ::memset(buffer_, 0, ROOT_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + Block::BLOCK_MAGIC_OFFSET,
        &Block::BLOCK_MAGIC_NUMBER,
        Block::BLOCK_MAGIC_SIZE);
    // 设定类型
    setType(type);
    // 设定时戳
    TimeStamp ts;
    ts.now();
    setTimeStamp(ts);
    //设定block数目
    setCnt(0);
    // 设置checksum
    setChecksum();
}

void MetaBlock::clear(unsigned int blockid)
{
    unsigned int spaceid = 0xffffffff; // -1表示meta
    blockid = htobe32(blockid);
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + BLOCK_MAGIC_OFFSET, &BLOCK_MAGIC_NUMBER, BLOCK_MAGIC_SIZE);
    // 设置spaceid
    ::memcpy(buffer_ + BLOCK_SPACEID_OFFSET, &spaceid, BLOCK_SPACEID_SIZE);
    // 设置blockid
    ::memcpy(buffer_ + BLOCK_NUMBER_OFFSET, &blockid, BLOCK_NUMBER_SIZE);
    // 设置usedspace
    setUsedspace(0);
    // 设置freespace
    unsigned short data = htobe16(META_DEFAULT_FREESPACE);
    ::memcpy(buffer_ + BLOCK_FREESPACE_OFFSET, &data, BLOCK_FREESPACE_SIZE);
    // 设定类型
    setType(BLOCK_TYPE_META);
    // 设置checksum
    setChecksum();
}
void DataBlock::clear(unsigned int blockid)
{
    unsigned int spaceid = 0x00000001; //!! 1表示data
    blockid = htobe32(blockid);
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + BLOCK_MAGIC_OFFSET, &BLOCK_MAGIC_NUMBER, BLOCK_MAGIC_SIZE);
    // 设置spaceid
    ::memcpy(buffer_ + BLOCK_SPACEID_OFFSET, &spaceid, BLOCK_SPACEID_SIZE);
    // 设置blockid
    ::memcpy(buffer_ + BLOCK_NUMBER_OFFSET, &blockid, BLOCK_NUMBER_SIZE);
    // 设置usedspace
    setUsedspace(0);
    // 设置freespace
    unsigned short data = htobe16(DATA_DEFAULT_FREESPACE);
    ::memcpy(buffer_ + BLOCK_FREESPACE_OFFSET, &data, BLOCK_FREESPACE_SIZE);
    //设定slots[]数目
    setSlotsNum(0);
    // 设定类型
    setType(BLOCK_TYPE_DATA);
    // 设置checksum
    setChecksum();
}
void IndexBlock::clear(unsigned int blockid)
{
    unsigned int spaceid = 0x00000002; //!! 2表示index
    blockid = htobe32(blockid);
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + BLOCK_MAGIC_OFFSET, &BLOCK_MAGIC_NUMBER, BLOCK_MAGIC_SIZE);
    // 设置spaceid
    ::memcpy(buffer_ + BLOCK_SPACEID_OFFSET, &spaceid, BLOCK_SPACEID_SIZE);
    // 设置blockid
    ::memcpy(buffer_ + BLOCK_NUMBER_OFFSET, &blockid, BLOCK_NUMBER_SIZE);
    // 设置usedspace
    setUsedspace(0);
    // 设置freespace
    unsigned short data = htobe16(INDEX_DEFAULT_FREESPACE);
    ::memcpy(buffer_ + BLOCK_FREESPACE_OFFSET, &data, BLOCK_FREESPACE_SIZE);
    //设定slots[]数目
    setSlotsNum(0);
    // 设定类型
    setType(BLOCK_TYPE_INDEX);
    // 设置checksum
    setChecksum();
}
bool Block::allocate(const unsigned char *header, struct iovec *iov, int iovcnt)
{
    // 判断是否有空间
    unsigned short length = getFreeLength();
    if (length == 0) return false;

    // 判断能否分配
    std::pair<size_t, size_t> ret = Record::size(iov, iovcnt);
    length -= 2; // 一个slot占2字节
    if (ret.first > length) {
        int usedspace = getUsedspace();
        if (ret.first < INITIAL_FREE_SPACE_SIZE - usedspace - 2) {
            rewrite();
            length = getFreeLength();
            if (length < 2) return false;
            length -= 2;
            if (ret.first > length) return false;
        } else
            return false;
    }

    // 写入记录
    Record record;
    unsigned short oldf = getFreespace();
    record.attach(buffer_ + oldf, length);
    unsigned short pos = (unsigned short) record.set(iov, iovcnt, header);

    // 调整usedspace
    int usedspace = getUsedspace();
    usedspace += pos;
    usedspace += 2;
    setUsedspace(usedspace);

    // 调整freespace
    setFreespace(pos + oldf);
    // 写slot
    unsigned short slots = getSlotsNum();
    setSlotsNum(slots + 1); // 增加slots数目
    setSlot(slots, oldf);   // 第slots个

    // slots未排序，同时需要setChecksum
    return true;
}
bool MetaBlock::allocate(
    const unsigned char *header,
    struct iovec *iov,
    int iovcnt)
{
    // 判断是否有空间
    unsigned short length = getFreeLength();
    if (length == 0) return false;

    // 判断能否分配
    std::pair<size_t, size_t> ret = Record::size(iov, iovcnt);
    length -= 2; // 一个slot占2字节
    if (ret.first > length) {
        int usedspace = getUsedspace();
        if (ret.first < INITIAL_FREE_SPACE_SIZE - usedspace - 2) {
            rewrite();
            length = getFreeLength();
            if (length < 2) return false;
            length -= 2;
            if (ret.first > length) return false;
        } else
            return false;
    }

    // 写入记录
    Record record;
    unsigned short oldf = getFreespace();
    record.attach(buffer_ + oldf, length);
    unsigned short pos = (unsigned short) record.set(iov, iovcnt, header);

    // 调整usedspace
    int usedspace = getUsedspace();
    usedspace += pos;
    usedspace += 2;
    setUsedspace(usedspace);

    // 调整freespace
    setFreespace(pos + oldf);
    // 写slot
    unsigned short slots = getSlotsNum();
    setSlotsNum(slots + 1); // 增加slots数目
    setSlot(slots, oldf);   // 第slots个

    // slots未排序，同时需要setChecksum
    return true;
}
bool DataBlock::allocate(
    const unsigned char *header,
    struct iovec *iov,
    int iovcnt)
{
    // 判断是否有空间
    unsigned short length = getFreeLength();
    if (length == 0) return false;

    // 判断能否分配
    std::pair<size_t, size_t> ret = Record::size(iov, iovcnt);
    length -= 2; // 一个slot占2字节
    if (ret.first > length) {
        int usedspace = getUsedspace();
        if (ret.first < INITIAL_FREE_SPACE_SIZE - usedspace - 2) {
            rewrite();
            length = getFreeLength();
            if (length < 2) return false;
            length -= 2;
            if (ret.first > length) return false;
        } else
            return false;
    }

    // 写入记录
    Record record;
    unsigned short oldf = getFreespace();
    record.attach(buffer_ + oldf, length);
    unsigned short pos = (unsigned short) record.set(iov, iovcnt, header);

    // 调整usedspace
    int usedspace = getUsedspace();
    usedspace += pos;
    usedspace += 2;
    setUsedspace(usedspace);

    // 调整freespace
    setFreespace(pos + oldf);
    // 写slot
    unsigned short slots = getSlotsNum();
    setSlotsNum(slots + 1); // 增加slots数目
    setSlot(slots, oldf);   // 第slots个

    // slots未排序，同时需要setChecksum
    return true;
}
bool IndexBlock::allocate(
    const unsigned char *header,
    struct iovec *iov,
    int iovcnt)
{
    // 判断是否有空间
    unsigned short length = getFreeLength();
    if (length == 0) return false;

    // 判断能否分配
    std::pair<size_t, size_t> ret = Record::size(iov, iovcnt);
    length -= 2; // 一个slot占2字节
    if (ret.first > length) {
        int usedspace = getUsedspace();
        if (ret.first < INITIAL_FREE_SPACE_SIZE - usedspace - 2) {
            rewrite();
            length = getFreeLength();
            if (length < 2) return false;
            length -= 2;
            if (ret.first > length) return false;
        } else
            return false;
    }

    // 写入记录
    Record record;
    unsigned short oldf = getFreespace();
    record.attach(buffer_ + oldf, length);
    unsigned short pos = (unsigned short) record.set(iov, iovcnt, header);

    // 调整usedspace
    int usedspace = getUsedspace();
    usedspace += pos;
    usedspace += 2;
    setUsedspace(usedspace);

    // 调整freespace
    setFreespace(pos + oldf);
    // 写slot
    unsigned short slots = getSlotsNum();
    setSlotsNum(slots + 1); // 增加slots数目
    setSlot(slots, oldf);   // 第slots个

    // slots未排序，同时需要setChecksum
    return true;
}
int Block::recDelete(struct iovec *keyField, RelationInfo *relationInfo)
{
    unsigned int key = relationInfo->key;
    unsigned short slotsNum = getSlotsNum();
    int deleteindex = -1;
    std::vector<unsigned short> slotsv;
    for (unsigned short index = 0; index < slotsNum; index++) {
        unsigned short recOffset = getSlot(index);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
        struct iovec field;
        record.specialRef(field, key);
        if (!(relationInfo->fields[key].type->compare(
                field.iov_base,
                keyField->iov_base,
                field.iov_len,
                keyField->iov_len)) &&
            !(relationInfo->fields[key].type->compare(
                keyField->iov_base,
                field.iov_base,
                keyField->iov_len,
                field.iov_len))) {
            deleteindex = index;
            // 调整usedspace
            int usedspace = getUsedspace();
            int recSize = ((int) record.length() + Record::ALIGN_SIZE - 1) /
                          Record::ALIGN_SIZE * Record::ALIGN_SIZE;
            usedspace -= recSize;
            usedspace -= 2;
            setUsedspace(usedspace);
            continue;
        }
        slotsv.push_back(recOffset);
    }
    if (deleteindex != -1) {
        // 调整slots
        setSlotsNum(--slotsNum);
        for (unsigned short index = 0; index < slotsNum; index++)
            setSlot(index, slotsv[index]);
    }
    return deleteindex;
}
int DataBlock::recDelete(struct iovec *keyField, RelationInfo *relationInfo)
{
    unsigned int key = relationInfo->key;
    unsigned short slotsNum = getSlotsNum();
    int deleteindex = -1;
    std::vector<unsigned short> slotsv;

    //逐个record进行判断
    for (unsigned short index = 0; index < slotsNum; index++) {
        unsigned short recOffset = getSlot(index);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
        struct iovec field;
        record.specialRef(field, key);

        //如果当前record的主键就是要删除record的主键
        if (!(relationInfo->fields[key].type->compare(
                field.iov_base,
                keyField->iov_base,
                field.iov_len,
                keyField->iov_len)) &&
            !(relationInfo->fields[key].type->compare(
                keyField->iov_base,
                field.iov_base,
                keyField->iov_len,
                field.iov_len))) {
            deleteindex = index; //确定了要删除record在block里的位置

            // 调整usedspace
            int usedspace = getUsedspace();
            int recSize = ((int) record.length() + Record::ALIGN_SIZE - 1) /
                          Record::ALIGN_SIZE * Record::ALIGN_SIZE;
            usedspace -= recSize;
            usedspace -= 2;
            setUsedspace(usedspace);
            continue; //不把删除的record的slots添加到vector容器中
        }
        slotsv.push_back(recOffset); //把slots加到vector容器中
    }
    if (deleteindex != -1) {
        // 调整slots
        setSlotsNum(--slotsNum);
        for (unsigned short index = 0; index < slotsNum; index++)
            setSlot(index, slotsv[index]); //写slots
    }
    return deleteindex; //返回删除record在block中的位置，-1代表失败
}
int IndexBlock::recDelete(struct iovec *keyField, RelationInfo *relationInfo)
{
    unsigned int key = 0;
    unsigned short slotsNum = getSlotsNum();
    int deleteindex = -1;
    std::vector<unsigned short> slotsv;
    for (unsigned short index = 0; index < slotsNum; index++) {
        unsigned short recOffset = getSlot(index);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
        struct iovec field;
        record.specialRef(field, key);
        if (!(relationInfo->fields[key].type->compare(
                field.iov_base,
                keyField->iov_base,
                field.iov_len,
                keyField->iov_len)) &&
            !(relationInfo->fields[key].type->compare(
                keyField->iov_base,
                field.iov_base,
                keyField->iov_len,
                field.iov_len))) {
            deleteindex = index;
            // 调整usedspace
            int usedspace = getUsedspace();
            int recSize = ((int) record.length() + Record::ALIGN_SIZE - 1) /
                          Record::ALIGN_SIZE * Record::ALIGN_SIZE;
            usedspace -= recSize;
            usedspace -= 2;
            setUsedspace(usedspace);
            continue;
        }
        slotsv.push_back(recOffset);
    }
    if (deleteindex != -1) {
        // 调整slots
        setSlotsNum(--slotsNum);
        for (unsigned short index = 0; index < slotsNum; index++)
            setSlot(index, slotsv[index]);
    }
    return deleteindex;
}
int Block::rewrite()
{
    Block upblock;
    unsigned char db[Block::BLOCK_SIZE];
    upblock.attach(db);
    upblock.clear(spaceid(), blockid());
    upblock.setNextid(getNextid());

    unsigned short slotsNum = getSlotsNum();
    for (unsigned short index = 0; index < slotsNum; index++) {
        unsigned short recOffset = getSlot(index);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
        // 先分配iovec
        size_t fields = record.fields();
        struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
        unsigned char header;
        // 从记录得到iovec
        record.ref(iov, (int) fields, &header);
        upblock.allocate(&header, iov, (int) fields);
        free(iov);
    }
    ::memcpy(buffer_, db, Block::BLOCK_SIZE);
    return S_OK;
}
int DataBlock::rewrite()
{
    DataBlock upblock;
    unsigned char db[Block::BLOCK_SIZE];
    upblock.attach(db);
    upblock.clear(blockid());
    upblock.setNextid(getNextid());

    unsigned short slotsNum = getSlotsNum();
    for (unsigned short index = 0; index < slotsNum; index++) {
        unsigned short recOffset = getSlot(index);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
        // 先分配iovec
        size_t fields = record.fields();
        struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
        unsigned char header;
        // 从记录得到iovec
        record.ref(iov, (int) fields, &header);
        upblock.allocate(&header, iov, (int) fields);
        free(iov);
    }
    ::memcpy(buffer_, db, Block::BLOCK_SIZE);
    return S_OK;
}
int IndexBlock::rewrite()
{
    IndexBlock upblock;
    unsigned char db[Block::BLOCK_SIZE];
    upblock.attach(db);
    upblock.clear(blockid());
    upblock.setNextid(getNextid());
    upblock.setNodeType(getNodeType());

    unsigned short slotsNum = getSlotsNum();
    for (unsigned short index = 0; index < slotsNum; index++) {
        unsigned short recOffset = getSlot(index);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
        // 先分配iovec
        size_t fields = record.fields();
        struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
        unsigned char header;
        // 从记录得到iovec
        record.ref(iov, (int) fields, &header);
        upblock.allocate(&header, iov, (int) fields);
        free(iov);
    }
    ::memcpy(buffer_, db, Block::BLOCK_SIZE);
    return S_OK;
}
} // namespace db
