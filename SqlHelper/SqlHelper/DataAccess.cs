using System;
using System.Data;
using System.Data.SqlClient;

namespace SqlHelper
{
    public class DataAccess
    {
        private readonly string ConnectionString;
        
        public DataAccess(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public void CloseConnection(SqlConnection connection)
        {
            connection.Close();
        }

        public void OpenConnection(SqlConnection connection)
        {
            connection.Open();
        }

        public SqlParameter CreateParameter(string name, object value, DbType dbType)
        {
            return CreateParameter(name, 0, value, dbType, ParameterDirection.Input);
        }

        public SqlParameter CreateParameter(string name, int size, object value, DbType dbType)
        {
            return CreateParameter(name, size, value, dbType, ParameterDirection.Input);
        }

        public SqlParameter CreateParameter(string name, int size, object value, DbType dbType, ParameterDirection direction)
        {
            return new SqlParameter
            {
                DbType = dbType,
                ParameterName = name,
                Size = size,
                Direction = direction,
                Value = value
            };
        }

        public DataTable GetDataTable(string commandText, CommandType commandType, SqlParameter[] parameters = null)
        {
            DataTable dataTable;

            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);
                
                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);
                    var dataset = new DataSet();
                    var dataAdapter = new SqlDataAdapter(command);
                    dataAdapter.Fill(dataset);
                    CloseConnection(connection);
                    dataTable = dataset.Tables[0];
                }

                CloseConnection(connection);
            }

            return dataTable;
        }

        /// <summary>
        /// Returns an IDataReader with an Open connection to a SQL datasource. NOTE: user must close connection after use.
        /// </summary>
        /// <param name="commandText"></param>
        /// <param name="commandType"></param>
        /// <param name="parameters"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        public IDataReader GetDataReader(string commandText, CommandType commandType, SqlParameter[] parameters, out SqlConnection connection)
        {
            connection = new SqlConnection(ConnectionString);
            connection.Open();

            var command = new SqlCommand(commandText, connection)
            {
                CommandType = commandType
            };

            AddParametersToCommand(command, parameters);
            IDataReader reader = command.ExecuteReader();

            return reader;
        }

        public void Delete(string commandText, CommandType commandType, SqlParameter[] parameters = null)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);
                
                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);
                    command.ExecuteNonQuery();
                }

                CloseConnection(connection);
            }
        }

        public void Insert(string commandText, CommandType commandType, SqlParameter[] parameters)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);
                    command.ExecuteNonQuery();
                }

                CloseConnection(connection);
            }
        }

        public int Insert(string commandText, CommandType commandType, SqlParameter[] parameters, out int lastId)
        {
            lastId = 0;

            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);
                    object newId = command.ExecuteScalar();
                    lastId = Convert.ToInt32(newId);
                }

                CloseConnection(connection);
            }

            return lastId;
        }

        public long Insert(string commandText, CommandType commandType, SqlParameter[] parameters, out long lastId)
        {
            lastId = 0;

            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);
                    object newId = command.ExecuteScalar();
                    lastId = Convert.ToInt64(newId);
                }

                CloseConnection(connection);
            }
            return lastId;
        }

        public void InsertWithTransaction(string commandText, CommandType commandType, SqlParameter[] parameters)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);
                SqlTransaction transactionScope = connection.BeginTransaction();

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);
                    
                    try
                    {
                        command.ExecuteNonQuery();
                        transactionScope.Commit();
                    }
                    catch (Exception)
                    {
                        transactionScope.Rollback();
                    }
                    finally
                    {
                        CloseConnection(connection);
                    }
                }
            }
        }

        public void InsertWithTransaction(string commandText, CommandType commandType, IsolationLevel isolationLevel, SqlParameter[] parameters)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);
                SqlTransaction transactionScope = connection.BeginTransaction(isolationLevel);

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);

                    try
                    {
                        command.ExecuteNonQuery();
                        transactionScope.Commit();
                    }
                    catch (Exception)
                    {
                        transactionScope.Rollback();
                    }
                    finally
                    {
                        CloseConnection(connection);
                    }
                }
            }
        }

        public void Update(string commandText, CommandType commandType, SqlParameter[] parameters)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);
                    command.ExecuteNonQuery();
                }

                CloseConnection(connection);
            }
        }

        public void UpdateWithTransaction(string commandText, CommandType commandType, SqlParameter[] parameters)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);
                SqlTransaction transactionScope = connection.BeginTransaction();

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);

                    try
                    {
                        command.ExecuteNonQuery();
                        transactionScope.Commit();
                    }
                    catch (Exception)
                    {
                        transactionScope.Rollback();
                    }
                    finally
                    {
                        CloseConnection(connection);
                    }
                }
            }
        }

        public void UpdateWithTransaction(string commandText, CommandType commandType, IsolationLevel isolationLevel, SqlParameter[] parameters)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);
                SqlTransaction transactionScope = connection.BeginTransaction(isolationLevel);

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);
                    
                    try
                    {
                        command.ExecuteNonQuery();
                        transactionScope.Commit();
                    }
                    catch (Exception)
                    {
                        transactionScope.Rollback();
                    }
                    finally
                    {
                        CloseConnection(connection);
                    }
                }
            }
        }

        public object GetScalarValue(string commandText, CommandType commandType, SqlParameter[] parameters = null)
        {
            object scalarValue;

            using (var connection = new SqlConnection(ConnectionString))
            {
                OpenConnection(connection);

                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandType = commandType;
                    AddParametersToCommand(command, parameters);
                    scalarValue = command.ExecuteScalar();
                }

                CloseConnection(connection);
            }

            return scalarValue;
        }

        /// <summary>
        /// Method will add any parameters to SqlCommand object and return the updated object
        /// </summary>
        /// <param name="command"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        private SqlCommand AddParametersToCommand(SqlCommand command, SqlParameter[] parameters = null)
        {
            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    command.Parameters.Add(parameter);
                }
            }
            return command;
        }
    }
}
