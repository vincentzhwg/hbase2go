package hbase2

import (
	"fmt"
	"net"
	"strconv"

	"git.apache.org/thrift.git/lib/go/thrift"
)

// remember to close trans
func HbaseOpenThriftConnection(hbaseHost string, hbasePort int) (trans *thrift.TSocket, client *THBaseServiceClient, err error) {
	trans, err = thrift.NewTSocket(net.JoinHostPort(hbaseHost, strconv.Itoa(hbasePort)))
	if err != nil {
		err = fmt.Errorf("error resolving address:%s", err)
		return
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client = NewTHBaseServiceClientFactory(trans, protocolFactory)
	err = trans.Open()
	return
}

func HbaseCloseThriftConnection(trans *thrift.TSocket) (err error) {
	trans.Close()
	return
}

// when key not exists, the value is nil
func HbaseGetKeyValues(thriftClient *THBaseServiceClient, table, tableCF, rowKey []byte) (rowValues [][]byte, err error) {
	tget := &TGet{
		Row: rowKey,
		Columns: []*TColumn{
			&TColumn{
				Family: tableCF,
				//Qualifier: qualifier,
			},
		},
	}
	if tresult, e := thriftClient.Get(table, tget); e != nil {
		err = e
		return
	} else {
		if 0 == len(tresult.ColumnValues) {
			return
		} else {
			rowValues = make([][]byte, 0, 20)
			for _, col := range tresult.ColumnValues {
				rowValues = append(rowValues, col.Value)
			}
			return
		}
	}
}

// when key not exists, the value is nil
func HbaseGetKeySingleValue(thriftClient *THBaseServiceClient, table, tableCF, rowKey, qualifier []byte) (rowValue []byte, err error) {
	tget := &TGet{
		Row: rowKey,
		Columns: []*TColumn{
			&TColumn{
				Family:    tableCF,
				Qualifier: qualifier,
			},
		},
	}
	if tresult, e := thriftClient.Get(table, tget); e != nil {
		err = e
		return
	} else {
		if 0 == len(tresult.ColumnValues) {
			return
		} else {
			rowValue = tresult.ColumnValues[0].Value
			return
		}
	}
}

func HbasePutRow(thriftClient *THBaseServiceClient, table, tableCF, rowKey, qualifier, rowValue []byte) (err error) {
	tput := &TPut{
		Row: rowKey,
		ColumnValues: []*TColumnValue{
			&TColumnValue{
				Family:    tableCF,
				Qualifier: qualifier,
				Value:     rowValue,
			},
		},
	}
	err = thriftClient.Put(table, tput)
	return
}

// increment
func HbaseIncrement(thriftClient *THBaseServiceClient, table, tableCF, incRowKey, qualifier []byte) (incValue []byte, err error) {
	tincrement := &TIncrement{
		Row: incRowKey,
		Columns: []*TColumnIncrement{
			&TColumnIncrement{
				Family:    tableCF,
				Qualifier: qualifier,
				Amount:    1,
			},
		},
	}
	if tresult, e := thriftClient.Increment(table, tincrement); e != nil {
		err = e
		return
	} else {
		incValue = tresult.ColumnValues[0].Value
		return
	}
}
