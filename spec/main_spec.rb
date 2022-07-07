describe 'database' do

  before do
    `rm -rf test.db`
  end

  def run_script(commands)
    raw_output = nil
    IO.popen("src/db test.db", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it 'keeps data after closing connection' do
    result1 = run_script([
      "insert 1 user1 person1@example.com",
      ".exit",
    ])
    expect(result1).to match_array([
      "db > Executed.",
      "db > ",
    ])
    
    result2 = run_script([
      "select",
      ".exit",
    ])
    expect(result2).to match_array([
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > ",
    ])
  end

  # part8 增加一个测试
  it 'allows printing out the structure of a one-node btree' do
    script = [3,1,2].map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".btree"
    script << ".exit"
    result = run_script(script)

    expect(result).to match_array([
      "db > Executed.",
      "db > Executed.",
      "db > Executed.",
      "db > Tree:",
      "leaf (size 3)",
      "  - 0 : 1",
      "  - 1 : 2",
      "  - 2 : 3",
      "db > "
    ])
    end

    # part9 增加一个测试
    it 'print an error message if there is a duplicate id' do
      script = [
        "insert 1 user1 person1@example.com",
        "insert 1 user1 person1@example.com",
        "select",
        ".exit"
      ]
      result = run_script(script)
      expect(result).to match_array([
        "db > Executed.",
        "db > Error: Duplicate key.",
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > ",
      ])
      end

      # 先添加一个测试，针对一个节点装满的情况
      # LEAF_NODE_SPACE_FOR_CELLS: 表示一个叶子结点留给真正数据的空间，是4086B
      # ROW_SIZE:每条记录的大小，是293B
      # 两者相除，得到内个叶子结点可以存储的数据条数，即4086 / 293 = 13余1，那就是说插入14个，就会溢出
      it 'print an error message if row of data oversize of a B+ tree node' do
        script = (1..14).map do |i|
          "insert #{i} user#{i} person#{i}@example.com"
        end

        script << ".exit"

        result = run_script(script)
        expect(result[-2]).to eq('db > Error: Table full.')
        end


  # add extra test example here

end
