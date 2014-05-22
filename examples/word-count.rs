extern crate map_reduce;
extern crate collections;

use map_reduce::MapReduce;
use map_reduce::Mapper;
use map_reduce::Reducer;
use map_reduce::PairList;
use map_reduce::Pair;

use collections::HashMap;

fn mapCountWords(_name: ~str, file: ~str) -> PairList {

    let mut occurences : PairList = vec!();

    for word in file.split(' ') {
        occurences.push(Pair{
            key: word.clone().to_owned(),
            value: 1
        });
    }

    return occurences;

}

fn reduceCountWords(data: PairList) -> PairList {

    let mut map : HashMap<~str, uint> = HashMap::new();

    for pair in data.iter() {

        if map.contains_key(&pair.key) {

            *map.get_mut(&pair.key) += pair.value;

        } else {

            map.insert(pair.key.clone(), pair.value);

        }

    }

    let mut list : PairList = vec!();
    for (key,value) in map.iter() {
        list.push(Pair{key:key.clone(),value:*value});
    }

    return list;
}

fn newStrPair(key: &'static str, value: &'static str) -> Pair<~str, ~str> {
    Pair{key: key.to_owned(), value: value.to_owned()}
}

#[test]
fn test_map_reduce_word_count() {

    let mut files : Vec<Pair<~str,~str>> = vec!();

    files.push(newStrPair("doc1", "test stra 1"));
    files.push(newStrPair("doc1", "test str 2"));
    files.push(newStrPair("doc1", "test str 3"));
    files.push(newStrPair("doc1", "test str 4"));

    let result = MapReduce(
        Mapper{f: mapCountWords},
        Reducer{f: reduceCountWords},
        files);

    let mut map : HashMap<~str, uint> = HashMap::new();
    for pair in result.iter() {
        map.insert(pair.key.clone(), pair.value);
    }

    assert_eq!(*map.get(&"test".to_owned()), 4);
    assert_eq!(*map.get(&"stra".to_owned()), 1);
    assert_eq!(*map.get(&"str".to_owned()), 3);
    assert_eq!(*map.get(&"1".to_owned()), 1);
    assert_eq!(*map.get(&"2".to_owned()), 1);
    assert_eq!(*map.get(&"3".to_owned()), 1);
    assert_eq!(*map.get(&"4".to_owned()), 1);

}
