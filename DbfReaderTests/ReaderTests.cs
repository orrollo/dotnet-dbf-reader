using System.IO;
using System.Reflection;
using NUnit.Framework;
using DotNetDbfReader;

namespace DotNetDbfReaderTests
{
	[TestFixture]
	public class ReaderTests
	{
		public static Stream GetDbfStream()
		{
			return Assembly
				.GetExecutingAssembly()
				.GetManifestResourceStream("DotNetDbfReaderTests.test.dbf");
		}

		[Test]
		public void TestGetStream()
		{
			var stream = GetDbfStream();
			Assert.AreNotEqual(null, stream);
		}

		[Test]
		public void TestOpenStream()
		{
			var rdr = new DbfReader(GetDbfStream());
			Assert.AreNotEqual(null, rdr);
		}

		protected void CheckField(DbfField field, string name, DbfType type, int length, int? decimals = null)
		{
			Assert.AreEqual(name, field.Name);
			Assert.AreEqual(type, field.DbfType);
			Assert.AreEqual(length, field.FieldLength);
			if (decimals != null) Assert.AreEqual(decimals, field.Decimals);
		}

		[Test]
		public void TestFields()
		{
			var rdr = new DbfReader(GetDbfStream());
			Assert.AreEqual(6, rdr.Fields.Length);

			CheckField(rdr.Fields[0], "NAME1", DbfType.Character, 128, 0);
			CheckField(rdr.Fields[1], "SCALERANK", DbfType.Numeric, 10, 0);
			CheckField(rdr.Fields[2], "COUNTRYNAM", DbfType.Character, 50, 0);
			CheckField(rdr.Fields[3], "FeatureCla", DbfType.Character, 30, 0);
			CheckField(rdr.Fields[4], "ADM0_A3", DbfType.Character, 3, 0);
			CheckField(rdr.Fields[5], "MAP_COLOR", DbfType.Numeric, 4, 0);
		}

		[Test]
		public void TestRecordCount()
		{
			var rdr = new DbfReader(GetDbfStream());
			uint count = 0;
			while (rdr.Read()) count++;
			Assert.AreEqual(rdr.RecordCount, count);
		}

		private object[][] testData =
			{
				new object[] {"US_100", "US_101", "US_102", "US_122", "US_121"},
				new object[] {9.0, 9.0, 9.0, 2.0, 2.0},
				new object[]
					{
						"United States of America", 
						"United States of America",
						"United States of America", 
						"United States of America", 
						"United States of America"
					},
				new object[]
					{
						"Adm-1 boundary water indicator", 
						"Adm-1 boundary water indicator",
						"Adm-1 boundary water indicator", 
						"Adm-1 boundary", 
						"Adm-1 boundary"
					},
				new object[] {"USA", "USA", "USA", "USA", "USA"},
				new object[] {1.0, 1.0, 1.0, 1.0, 1.0}
			};

		[Test]
		public void TestRecords()
		{
			var rdr = new DbfReader(GetDbfStream());
			Assert.AreEqual(6, rdr.Fields.Length);
			for (int i = 0; i < 5; i++)
			{
				Assert.AreEqual(true, rdr.Read());
				for (int j = 0; j < 6; j++) Assert.AreEqual(testData[j][i], rdr[j]);
			}
		}
	}
}
