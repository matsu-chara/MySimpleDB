package plan;

import parser.CreateIndexData;
import parser.CreateTableData;
import parser.CreateViewData;
import parser.DeleteData;
import parser.ModifyData;
import tx.Transaction;

public interface UpdatePlanner {
  public int executeDelete(DeleteData data, Transaction tx);

  public int executeModify(ModifyData data, Transaction tx);

  public int executeCreateTable(CreateTableData data, Transaction tx);

  public int executeCreateView(CreateViewData data, Transaction tx);

  public int executeIndex(CreateIndexData data, Transaction tx);
}
