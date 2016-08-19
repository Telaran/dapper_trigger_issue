using Dapper;
using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper_Issue
{
    class Program
    {
        [Dapper.Contrib.Extensions.Table("StuffTrigger")]
        public class StuffTrigger
        {
            [Dapper.Contrib.Extensions.Key]
            public short TheId { get; set; }
            public short ASecondKey { get; set; }
            public string Name { get; set; }
            public DateTime? Created { get; set; }
        }



        static void Main(string[] args)
        {
            using (var connection = new SqlConnection("Data Source=.;Initial Catalog=tempdb;Integrated Security=True"))
            {
                Action<string> dropTable = name => connection.Execute($@"IF OBJECT_ID('{name}', 'U') IS NOT NULL DROP TABLE [{name}]; ");
                dropTable("StuffTrigger");
                connection.Execute(@"CREATE TABLE StuffTrigger (TheId int IDENTITY(1,1) not null, ASecondKey Int, Name nvarchar(100) not null, Created DateTime null);");
                
                try
                {
                    connection.Execute("DROP TRIGGER [dbo].[trg_DIExample]");
                }
                catch (Exception)
                {
                }
                connection.Execute(@"CREATE TRIGGER [dbo].[trg_DIExample]
                                       ON [dbo].[StuffTrigger]
                                     INSTEAD OF INSERT AS
                                       BEGIN
                                         SET NOCOUNT ON;
                                         WITH ChangeList AS
                                         (SELECT
                                            Inserted.ASecondKey AS Inserted_Index,
                                            Inserted.Name,
                                            Inserted.Created,
                                            Deleted.ASecondKey  AS Deleted_Index
                                          FROM Inserted
                                            FULL OUTER JOIN Deleted ON Deleted.ASecondKey = Inserted.ASecondKey
                                         )
                                         MERGE
                                         INTO dbo.StuffTrigger AS Tgt
                                         USING ChangeList AS Src
                                         ON
                                           Src.Inserted_Index = Tgt.ASecondKey
                                         WHEN MATCHED
                                         THEN
                                         UPDATE SET

                                           TGT.ASecondKey = SRC.Inserted_Index,
                                           TGT.Name       = SRC.Name,
                                           TGT.Created    = SRC.Created

                                         WHEN NOT MATCHED
                                         THEN
                                         INSERT
                                           (
                                             ASecondKey,
                                             Name,
                                             Created
                                           )
                                           VALUES
                                             (
                                               SRC.Inserted_Index,
                                               SRC.Name,
                                               SRC.Created
                                             )

                                         WHEN NOT MATCHED BY SOURCE AND tgt.ASecondKey = (SELECT ASecondKey
                                                                                          FROM Deleted)
                                         THEN
                                         DELETE;

                                       END");

                for (int i = 1; i <= 5; i++)
                {
                    var stuffTrigerEx = new StuffTrigger()
                    {
                        Created = DateTime.UtcNow,
                        Name = $"Test_{DateTime.UtcNow.Ticks}"
                    };

                    var idResult = connection.Insert(stuffTrigerEx);
                    Console.WriteLine($"[{i}]: {idResult}");
                }
                Console.ReadKey();
            }
        }
    }
}
