package jdbc.network;

import static java.sql.Types.INTEGER;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import record.Schema;

public class RemoteMetaDataImpl extends UnicastRemoteObject implements RemoteMetaData {
  private Schema sch;
  private List<String> fields = new ArrayList<>();

  public RemoteMetaDataImpl(Schema sch) throws RemoteException {
    this.sch = sch;
    fields.addAll(sch.fields());
  }

  @Override
  public int getColumnCount() throws RemoteException {
    return fields.size();
  }

  @Override
  public String getColumnName(int column) throws RemoteException {
    return fields.get(column - 1);
  }

  @Override
  public int getColumnType(int column) throws RemoteException {
    var fldname = getColumnName(column);
    return sch.type(fldname);
  }

  @Override
  public int getColumnDisplaySize(int column) throws RemoteException {
    var fldname = getColumnName(column);
    var fldtype = sch.type(fldname);
    var fldlength = (fldtype == INTEGER) ? 6 : sch.length(fldname);
    return Math.max(fldname.length(), fldlength) + 1;
  }
}
